 import numpy as np
import os
from transformers import T5ForConditionalGeneration, Trainer, TrainingArguments
from sklearn.model_selection import KFold
import evaluate
import torch
from embed import tokenizer
import datetime
import warnings
import json
import shutil

warnings.filterwarnings("ignore")

def compute_metrics(eval_preds):
    preds, labels = eval_preds
    if isinstance(preds, tuple):
        preds = preds[0]
    decoded_preds = tokenizer.batch_decode(preds, skip_special_tokens=True)
    labels = np.where(labels != -100, labels, tokenizer.pad_token_id)
    decoded_labels = tokenizer.batch_decode(labels, skip_special_tokens=True)
    
    metric = evaluate.load("rouge")
    result = metric.compute(predictions=decoded_preds, references=decoded_labels, use_stemmer=True)
    
    return {
        'rouge1': result['rouge1'].mid.fmeasure,
        'rouge2': result['rouge2'].mid.fmeasure,
        'rougeL': result['rougeL'].mid.fmeasure,
    }

def train(train_dataset, eval_dataset, params, data_collator):
    model = T5ForConditionalGeneration.from_pretrained('t5-base')
    
    training_args = TrainingArguments(
        output_dir='./results',
        num_train_epochs=params['num_train_epochs'],
        per_device_train_batch_size=params['batch_size'],
        per_device_eval_batch_size=params['batch_size'],
        learning_rate=params['learning_rate'],
        weight_decay=params['weight_decay'],
        logging_dir='./logs',
        logging_steps=100,
        evaluation_strategy="epoch",
        save_strategy="epoch",
        load_best_model_at_end=True,
        metric_for_best_model="eval_loss",
        greater_is_better=False,
    )
    
    trainer = Trainer(
        model=model,
        args=training_args,
        train_dataset=train_dataset,
        eval_dataset=eval_dataset,
        compute_metrics=compute_metrics,
        data_collator=data_collator,
    )
    
    trainer.train()
    return trainer.model

def cross_validation(dataset, eval_dataset, params, model_save_path, data_collator, n_splits=5):
    kf = KFold(n_splits=n_splits, shuffle=True, random_state=42)
    
    rouge1_list, rouge2_list, rougeL_list = [], [], []
    best_model = None
    best_rouge_avg = -1
    
    for fold, (train_index, val_index) in enumerate(kf.split(dataset)):
        print(f"Training fold {fold + 1}/{n_splits}")
        
        train_dataset = [dataset[i] for i in train_index]
        val_dataset = [dataset[i] for i in val_index]
        
        model = train(train_dataset, eval_dataset, params, data_collator)
        
        # Validate model
        trainer = Trainer(
            model=model, 
            compute_metrics=compute_metrics,
            data_collator=data_collator
        )
        metrics = trainer.evaluate(val_dataset)
        
        rouge1_list.append(metrics['eval_rouge1'])
        rouge2_list.append(metrics['eval_rouge2'])
        rougeL_list.append(metrics['eval_rougeL'])
        
        print(f"Fold {fold + 1} - ROUGE-1: {metrics['eval_rouge1']:.3f}, ROUGE-2: {metrics['eval_rouge2']:.3f}, ROUGE-L: {metrics['eval_rougeL']:.3f}")
        
        # Check if this model is the best so far
        current_rouge_avg = (metrics['eval_rouge1'] + metrics['eval_rouge2'] + metrics['eval_rougeL']) / 3
        if current_rouge_avg > best_rouge_avg:
            best_rouge_avg = current_rouge_avg
            best_model = model
    
    # Save the best model
    if best_model:
        best_model.save_pretrained(model_save_path)
        print(f"Best model saved to {model_save_path}")
    
    return np.mean(rouge1_list), np.mean(rouge2_list), np.mean(rougeL_list)

def optimize_hyperparameters(dataset, eval_dataset, model_save_path, data_collator):
    learning_rate_candidates = [1e-5, 3e-5, 5e-5]
    batch_size_candidates = [4, 8, 16]
    num_epochs_candidates = [3, 5, 10]
    weight_decay_candidates = [0.01, 0.1]
    
    best_params = None
    best_rouge_avg = -1
    best_model_path = None
    
    for lr in learning_rate_candidates:
        for bs in batch_size_candidates:
            for epochs in num_epochs_candidates:
                for wd in weight_decay_candidates:
                    params = {
                        'learning_rate': lr,
                        'batch_size': bs,
                        'num_train_epochs': epochs,
                        'weight_decay': wd,
                    }
                    
                    current_model_path = os.path.join(model_save_path, f"lr{lr}_bs{bs}_epochs{epochs}_wd{wd}")
                    os.makedirs(current_model_path, exist_ok=True)
                    
                    rouge1, rouge2, rougeL = cross_validation(dataset, eval_dataset, params, current_model_path, data_collator)
                    current_rouge_avg = (rouge1 + rouge2 + rougeL) / 3
                    
                    print(f"Params: {params}")
                    print(f"Results: ROUGE-1={rouge1:.3f}, ROUGE-2={rouge2:.3f}, ROUGE-L={rougeL:.3f}")
                    
                    if current_rouge_avg > best_rouge_avg:
                        best_rouge_avg = current_rouge_avg
                        best_params = params.copy()
                        best_model_path = current_model_path
    
    # Move the best model to the final location
    final_best_model_path = os.path.join(model_save_path, "best_model")
    if os.path.exists(final_best_model_path):
        shutil.rmtree(final_best_model_path)
    shutil.move(best_model_path, final_best_model_path)
    
    # Save best parameters
    with open(os.path.join(model_save_path, "best_params.json"), "w") as f:
        json.dump(best_params, f)
    
    print(f"Best model saved to {final_best_model_path}")
    print(f"Best parameters: {best_params}")
    
    return best_params

if __name__ == "__main__":
    print("This script is not meant to be run directly. Please use main.py to start the training process.")
