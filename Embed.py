from sentence_transformers import SentenceTransformer
import numpy as np
import pandas as pd
import re
import random

print("schema_matching|Loading sentence transformer, this will take a while...")
model = SentenceTransformer('paraphrase-multilingual-mpnet-base-v2')
print("schema_matching|Done loading sentence transformer")

def preprocess_sql(sql):
    """Preprocess SQL query."""
    sql = sql.lower()
    sql = re.sub(r'\s+', ' ', sql)  # Replace multiple spaces with single space
    return sql.strip()

def extract_sql_features(sql):
    """Extract additional features from SQL query."""
    features = {}
    features['table_count'] = len(re.findall(r'\bfrom\b|\bjoin\b', sql))
    features['where_count'] = len(re.findall(r'\bwhere\b', sql))
    features['group_by_count'] = len(re.findall(r'\bgroup by\b', sql))
    features['order_by_count'] = len(re.findall(r'\border by\b', sql))
    features['having_count'] = len(re.findall(r'\bhaving\b', sql))
    features['subquery_count'] = len(re.findall(r'\(select\b', sql))
    return features

def embed_sql(sql):
    """Embed SQL query and combine with extracted features."""
    sql = preprocess_sql(sql)
    embedding = model.encode(sql)
    features = extract_sql_features(sql)
    return np.concatenate([embedding, list(features.values())])

def embed_lineage(lineage):
    """Embed lineage string."""
    return model.encode(lineage)

def prepare_data(sql_file, lineage_file):
    """Prepare data by embedding SQL statements and lineages."""
    with open(sql_file, 'r') as f:
        sql_statements = f.readlines()
    with open(lineage_file, 'r') as f:
        lineages = f.readlines()
    
    sql_embeddings = np.array([embed_sql(sql) for sql in sql_statements])
    lineage_embeddings = np.array([embed_lineage(lineage) for lineage in lineages])
    
    return sql_embeddings, lineage_embeddings, lineages

def generate_sample_data(num_samples=1000):
    """Generate sample SQL and lineage data for testing."""
    tables = ['customers', 'orders', 'products', 'employees', 'suppliers']
    sql_statements = []
    lineages = []
    
    for _ in range(num_samples):
        used_tables = random.sample(tables, random.randint(1, len(tables)))
        sql = f"SELECT * FROM {used_tables[0]}"
        for table in used_tables[1:]:
            sql += f" JOIN {table} ON {used_tables[0]}.id = {table}.{used_tables[0]}_id"
        if random.choice([True, False]):
            sql += f" WHERE {random.choice(used_tables)}.id > {random.randint(1, 100)}"
        sql_statements.append(sql)
        lineages.append(" -> ".join(used_tables) + " -> Result")
    
    return sql_statements, lineages

if __name__ == "__main__":
    # Generate sample data
    sql_statements, lineages = generate_sample_data()
    
    # Save sample data
    with open('sample_sql.txt', 'w') as f:
        f.write('\n'.join(sql_statements))
    with open('sample_lineage.txt', 'w') as f:
        f.write('\n'.join(lineages))
    
    # Test embedding
    sql_embeddings, lineage_embeddings, _ = prepare_data('sample_sql.txt', 'sample_lineage.txt')
    print("SQL Embedding shape:", sql_embeddings.shape)
    print("Lineage Embedding shape:", lineage_embeddings.shape)
