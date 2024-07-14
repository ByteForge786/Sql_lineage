import numpy as np
import xgboost as xgb
from embed import embed_sql, embed_lineage
import os

def load_models(model_path):
    """Load all trained models and thresholds."""
    models = []
    thresholds = []
    for file in os.listdir(model_path):
        if file.endswith(".model"):
            model = xgb.Booster()
            model.load_model(os.path.join(model_path, file))
            models.append(model)
        elif file.endswith(".threshold"):
            with open(os.path.join(model_path, file), 'r') as f:
                thresholds.append(float(f.read()))
    return models, thresholds

def predict_lineage(sql, models, thresholds, label_mapping):
    """Predict lineage for a given SQL statement using ensemble of models."""
    sql_embedding = embed_sql(sql)
    lineage_embedding = np.zeros_like(embed_lineage("dummy"))  # Placeholder for inference
    
    features = np.concatenate([sql_embedding, lineage_embedding])
    dtest = xgb.DMatrix([features])
    
    predictions = []
    for model, threshold in zip(models, thresholds):
        pred = model.predict(dtest)
        pred_label = 1 if pred[0] > threshold else 0
        predictions.append(pred_label)
    
    # Use majority voting
    final_prediction = 1 if sum(predictions) > len(predictions) / 2 else 0
    
    return label_mapping[final_prediction]

if __name__ == "__main__":
    # Load models, thresholds, and label mapping
    model_path = "model/2023-07-14-12-34-56"  # Replace with your actual model path
    models, thresholds = load_models(model_path)
    label_mapping = np.load(f"{model_path}/label_mapping.npy")
    
    # Test prediction
    test_sql = "SELECT * FROM customers JOIN orders ON customers.id = orders.customer_id WHERE orders.total > 100"
    predicted_lineage = predict_lineage(test_sql, models, thresholds, label_mapping)
    
    print(f"SQL: {test_sql}")
    print(f"Predicted Lineage: {predicted_lineage}")
