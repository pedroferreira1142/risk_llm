import torch
from google.cloud import bigquery
import pandas as pd

# Initialize BigQuery client
client = bigquery.Client()

# Fetch project data
project_data_query = """
SELECT *
FROM `globis-code-wizard.project_data.project_data_pid`
"""
project_data = client.query(project_data_query).to_dataframe()

# Combine all columns into a single 'project_description' column
project_data['project_description'] = project_data.apply(
    lambda row: ' '.join([f'{col}: "{row[col]}"' for col in project_data.columns if col != 'document_id']),
    axis=1
)

# Fetch risk data
risk_data_query = """
SELECT *
FROM `globis-code-wizard.project_data.risk_data`
"""
risk_data = client.query(risk_data_query).to_dataframe()

# Group risks by document_id, concatenating risks, probabilities, impacts, and mitigation strategies into structured format
def format_risks(group):
    risks = []
    for _, row in group.iterrows():
        risks.append(f"Risk: {row['risk']}, Probability: {row['probability']}, Impact: {row['impact']}, Mitigation: {row['mitigation_strategy']}")
    return ' '.join(risks)

risk_data_grouped = risk_data.groupby('document_id').apply(format_risks).reset_index(name='formatted_risks')

# Merge the project and grouped risk data on the document_id
merged_data = pd.merge(project_data, risk_data_grouped, on='document_id')

# Create input and output columns for the model
merged_data['input_text'] = "Generate a list of risks, with the risk, impact, probability, and mitigation strategy: " + merged_data['project_description']
merged_data['target_text'] = merged_data['formatted_risks']

# Set pandas options to avoid truncating long strings
#pd.set_option('display.max_colwidth', None)  # Set None for unlimited column width
#pd.set_option('display.max_rows', None)      # Show all rows in the DataFrame
#pd.set_option('display.max_columns', None)   # Show all columns in the DataFrame


# Display the merged data
#print(merged_data['target_text'].head())

from datasets import Dataset

# Create a Hugging Face Dataset for fine-tuning
dataset = Dataset.from_pandas(merged_data[['input_text', 'target_text']])

#print(dataset)

from transformers import T5ForConditionalGeneration, T5Tokenizer, Trainer, TrainingArguments

# Load the T5 model and tokenizer
model_name = "t5-base"
tokenizer = T5Tokenizer.from_pretrained(model_name)
model = T5ForConditionalGeneration.from_pretrained(model_name)

# Move model to GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model = model.to(device)

# Tokenize the input and output data with explicit padding
def preprocess_function(examples):
    inputs = tokenizer(examples['input_text'], max_length=512, padding='max_length', truncation=True)
    targets = tokenizer(examples['target_text'], max_length=512, padding='max_length', truncation=True)
    
    # Replace padding token id in labels with -100 to ignore padding in the loss computation
    inputs['labels'] = targets['input_ids']
    inputs['labels'] = [[-100 if token == tokenizer.pad_token_id else token for token in labels] for labels in inputs['labels']]
    
    return inputs

# Apply preprocessing to the dataset
tokenized_dataset = dataset.map(preprocess_function, batched=True)

# Split the data into training and validation sets
train_test_split = tokenized_dataset.train_test_split(test_size=0.1)
train_dataset = train_test_split['train']
eval_dataset = train_test_split['test']

# Move datasets to GPU (this happens automatically during training with the Trainer API, but for custom code, you'd do this)
# trainer handles the data transfer to the device automatically

# Define the training arguments
training_args = TrainingArguments(
    output_dir="./results",
    evaluation_strategy="epoch",
    per_device_train_batch_size=4,
    per_device_eval_batch_size=4,
    num_train_epochs=3,
    save_total_limit=2,
    fp16=True,  # Enable mixed precision
    logging_dir="./logs",
    logging_steps=10,
)

# Initialize the Trainer
trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
)

# Fine-tune the model
trainer.train()
