import os
from transformers import T5ForConditionalGeneration, T5Tokenizer
import json
import torch
from collections import Counter

def load_models(model_path):
    """Load all trained models."""
    models = []
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    for folder in os.listdir(model_path):
        if os.path.isdir(os.path.join(model_path, folder)) and folder.isdigit():
            model = T5ForConditionalGeneration.from_pretrained(os.path.join(model_path, folder))
            model.to(device)
            model.eval()
            models.append(model)
    return models

def predict_lineage(sql, models, tokenizer):
    """Predict lineage for a given SQL statement using ensemble of models."""
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    inputs = tokenizer("Generate SQL lineage: " + sql, return_tensors="pt", max_length=512, padding="max_length", truncation=True)
    input_ids = inputs.input_ids.to(device)
    attention_mask = inputs.attention_mask.to(device)
    
    predictions = []
    with torch.no_grad():
        for model in models:
            output = model.generate(input_ids, attention_mask=attention_mask, max_length=512, num_return_sequences=1, num_beams=4)
            prediction = tokenizer.decode(output[0], skip_special_tokens=True)
            predictions.append(prediction)
    
    # Use the most common prediction as the final result
    return Counter(predictions).most_common(1)[0][0]

if __name__ == "__main__":
    # Load the latest model
    model_dirs = [d for d in os.listdir("model") if os.path.isdir(os.path.join("model", d))]
    latest_model_dir = max(model_dirs, key=lambda x: os.path.getctime(os.path.join("model", x)))
    model_path = os.path.join("model", latest_model_dir)

    print(f"Loading model from: {model_path}")
    models = load_models(model_path)
    tokenizer = T5Tokenizer.from_pretrained('t5-base')

    # Load best parameters
    with open(f"{model_path}/best_params.json", 'r') as f:
        best_params = json.load(f)

    print("Best parameters:", best_params)

    # Test prediction
    test_sql = """
    WITH cte1 AS (
        SELECT customer_id, SUM(amount) AS total_spent
        FROM orders
        WHERE order_date >= '2023-01-01'
        GROUP BY customer_id
    ),
    cte2 AS (
        SELECT customer_id, COUNT(order_id) AS order_count
        FROM orders
        WHERE order_date >= '2023-01-01'
        GROUP BY customer_id
    ),
    cte3 AS (
        SELECT c.customer_id, c.customer_name, cte1.total_spent, cte2.order_count
        FROM customers c
        LEFT JOIN cte1 ON c.customer_id = cte1.customer_id
        LEFT JOIN cte2 ON c.customer_id = cte2.customer_id
    )
    SELECT cte3.customer_id, cte3.customer_name, cte3.total_spent, cte3.order_count, r.region_name
    FROM cte3
    JOIN regions r ON cte3.customer_id = r.customer_id
    WHERE cte3.total_spent > 500
    ORDER BY cte3.total_spent DESC;
    """

    predicted_lineage = predict_lineage(test_sql, models, tokenizer)
    
    print(f"SQL:\n{test_sql}")
    print(f"\nPredicted Lineage:\n{predicted_lineage}")

# Function to evaluate model performance on a test set
def evaluate_model(model_path, test_sql_file, test_lineage_file):
    models = load_models(model_path)
    tokenizer = T5Tokenizer.from_pretrained('t5-base')
    
    with open(test_sql_file, 'r') as f:
        test_sql_statements = f.readlines()
    with open(test_lineage_file, 'r') as f:
        test_lineages = f.readlines()

    correct_predictions = 0
    total_predictions = len(test_sql_statements)

    for sql, true_lineage in zip(test_sql_statements, test_lineages):
        predicted_lineage = predict_lineage(sql, models, tokenizer)
        if predicted_lineage.strip() == true_lineage.strip():
            correct_predictions += 1

    accuracy = correct_predictions / total_predictions
    print(f"Model Accuracy: {accuracy:.4f}")

    return accuracy

# If you have a separate test set, you can evaluate the model like this:
# evaluate_model(model_path, "test_sql.txt", "test_lineage.txt")
