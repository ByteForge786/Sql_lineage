import numpy as np
import os
import xgboost as xgb
from sklearn.model_selection import KFold
from sklearn.metrics import f1_score, precision_score, recall_score
from embed import prepare_data
import datetime
import warnings
warnings.filterwarnings("ignore")

def train(train_features, train_labels, params, num_round=100):
    dtrain = xgb.DMatrix(train_features, label=train_labels)
    bst = xgb.train(params, dtrain, num_round)
    
    # Get best threshold
    best_f1 = 0
    best_threshold = 0
    for threshold in range(100):
        threshold = threshold / 100
        pred_labels = np.where(bst.predict(dtrain) > threshold, 1, 0)
        f1 = f1_score(train_labels, pred_labels, average="binary", pos_label=1)
        if f1 > best_f1:
            best_f1 = f1
            best_threshold = threshold
    return bst, best_threshold

def test(bst, best_threshold, test_features, test_labels):
    dtest = xgb.DMatrix(test_features, label=test_labels)
    pred = bst.predict(dtest)
    pred_labels = np.where(pred > best_threshold, 1, 0)
    precision = precision_score(test_labels, pred_labels, average="binary", pos_label=1)
    recall = recall_score(test_labels, pred_labels, average="binary", pos_label=1)
    f1 = f1_score(test_labels, pred_labels, average="binary", pos_label=1)
    return precision, recall, f1

def cross_validation(features, labels, params, num_round=100, n_splits=5):
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    
    precision_list = []
    recall_list = []
    f1_list = []
    
    for fold, (train_index, val_index) in enumerate(kf.split(features)):
        print(f"Training fold {fold + 1}/{n_splits}")
        
        X_train, X_val = features[train_index], features[val_index]
        y_train, y_val = labels[train_index], labels[val_index]
        
        model, best_threshold = train(X_train, y_train, params, num_round)
        
        # Save model and threshold
        model.save_model(f"{model_save_path}/{fold}.model")
        with open(f"{model_save_path}/{fold}.threshold", 'w') as f:
            f.write(str(best_threshold))
        
        # Validate model
        precision, recall, f1 = test(model, best_threshold, X_val, y_val)
        precision_list.append(precision)
        recall_list.append(recall)
        f1_list.append(f1)
        
        print(f"Fold {fold + 1} - Precision: {precision:.3f}, Recall: {recall:.3f}, F1: {f1:.3f}")
    
    return np.mean(precision_list), np.mean(recall_list), np.mean(f1_list)

def optimize_hyperparameters(features, labels):
    eta_candidates = [0.01, 0.05, 0.1, 0.3]
    max_depth_candidates = [3, 4, 5, 6]
    num_round_candidates = [100, 200, 300, 400]
    
    best_params = None
    best_f1 = 0
    
    for eta in eta_candidates:
        for max_depth in max_depth_candidates:
            for num_round in num_round_candidates:
                params = {
                    'max_depth': max_depth,
                    'eta': eta,
                    'objective': 'binary:logistic',
                    'eval_metric': 'logloss',
                }
                
                precision, recall, f1 = cross_validation(features, labels, params, num_round)
                
                print(f"Params: eta={eta}, max_depth={max_depth}, num_round={num_round}")
                print(f"Results: Precision={precision:.3f}, Recall={recall:.3f}, F1={f1:.3f}")
                
                if f1 > best_f1:
                    best_f1 = f1
                    best_params = params.copy()
                    best_params['num_round'] = num_round
    
    return best_params

if __name__ == "__main__":
    # Prepare data
    sql_file = "sample_sql.txt"
    lineage_file = "sample_lineage.txt"
    sql_embeddings, lineage_embeddings, lineages = prepare_data(sql_file, lineage_file)
    
    # Combine SQL and lineage embeddings
    features = np.concatenate([sql_embeddings, lineage_embeddings], axis=1)
    
    # Convert lineages to numerical labels
    unique_lineages = list(set(lineages))
    labels = np.array([unique_lineages.index(lineage) for lineage in lineages])
    
    # Create model save directory
    model_save_path = f"model/{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    os.makedirs(model_save_path, exist_ok=True)
    
    # Optimize hyperparameters
    best_params = optimize_hyperparameters(features, labels)
    
    # Final training with best parameters
    print("Training final models with best parameters...")
    precision, recall, f1 = cross_validation(features, labels, best_params, best_params['num_round'])
    
    print(f"Final results - Precision: {precision:.3f}, Recall: {recall:.3f}, F1: {f1:.3f}")
    
    # Save label mapping
    np.save(f"{model_save_path}/label_mapping.npy", np.array(unique_lineages))
    
    print("Training complete. Models and label mapping saved.")
