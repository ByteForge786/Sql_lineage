import os
from embed import prepare_data, tokenizer
from train import optimize_hyperparameters, cross_validation
import json
import datetime
from transformers import DataCollatorForSeq2Seq

def process_subfolder(subfolder_path):
    sql_file = os.path.join(subfolder_path, "sql.txt")
    output_file = os.path.join(subfolder_path, "output.txt")
    
    if os.path.exists(sql_file) and os.path.exists(output_file):
        with open(sql_file, 'r') as f:
            sql_statement = f.read().strip()
        with open(output_file, 'r') as f:
            lineage = f.read().strip()
        return sql_statement, lineage
    return None, None

def main():
    input_folder = "Input"  # Change this to your input folder path
    all_datasets = []

    for subfolder in os.listdir(input_folder):
        subfolder_path = os.path.join(input_folder, subfolder)
        if os.path.isdir(subfolder_path):
            print(f"Processing subfolder: {subfolder}")
            sql_statement, lineage = process_subfolder(subfolder_path)
            
            if sql_statement is None or lineage is None:
                print(f"Skipping subfolder {subfolder}: Missing sql.txt or output.txt")
                continue
            
            print("Preparing data...")
            dataset = prepare_data([sql_statement], [lineage])
            all_datasets.extend(dataset)

            # Print an example of one input
            print("\nExample of input from this subfolder:")
            print("SQL statement:")
            print(sql_statement)
            print("\nLineage:")
            print(lineage)
            print("\nTokenized input:")
            print(dataset[0])
            print("=" * 50)

    if not all_datasets:
        print("No valid data found in any subfolder. Exiting.")
        return

    # Create model save directory
    model_save_path = f"model/{datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')}"
    os.makedirs(model_save_path, exist_ok=True)
    
    # Create data collator
    data_collator = DataCollatorForSeq2Seq(tokenizer=tokenizer, model="t5-base")
    
    # Optimize hyperparameters
    print("Optimizing hyperparameters...")
    best_params = optimize_hyperparameters(all_datasets, model_save_path, data_collator)
    print("Best parameters:", best_params)
    
    # Save best parameters
    with open(f"{model_save_path}/best_params.json", 'w') as f:
        json.dump(best_params, f)
    
    # Final training with best parameters
    print("Training final model with best parameters...")
    rouge1, rouge2, rougeL = cross_validation(all_datasets, best_params, model_save_path, data_collator)
    
    print(f"Final results - ROUGE-1: {rouge1:.3f}, ROUGE-2: {rouge2:.3f}, ROUGE-L: {rougeL:.3f}")
    
    print("Training complete. Model, tokenizer, and best parameters saved.")
    print(f"Model save path: {model_save_path}")

if __name__ == "__main__":
    main()
