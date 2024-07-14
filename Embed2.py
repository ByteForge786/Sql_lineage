from transformers import T5Tokenizer
import torch

print("schema_matching|Loading T5 tokenizer, this will take a while...")
tokenizer = T5Tokenizer.from_pretrained('t5-base')
print("schema_matching|Done loading T5 tokenizer")

def prepare_data(sql_statements, lineages):
    """Prepare data by tokenizing SQL statements and lineages."""
    dataset = []
    for sql, lineage in zip(sql_statements, lineages):
        input_text = "Generate SQL lineage: " + sql
        input_encodings = tokenizer(input_text, padding="max_length", truncation=True, max_length=1024)
        
        with tokenizer.as_target_tokenizer():
            target_encodings = tokenizer(lineage, padding="max_length", truncation=True, max_length=512)
        
        dataset.append({
            "input_ids": input_encodings["input_ids"],
            "attention_mask": input_encodings["attention_mask"],
            "labels": target_encodings["input_ids"]
        })
    
    return dataset

if __name__ == "__main__":
    # Test data preparation
    sample_sql = ["SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"]
    sample_lineage = ["table1.id -> table2.id"]
    dataset = prepare_data(sample_sql, sample_lineage)
    print("Sample dataset entry:", dataset[0])
