# Assuming fine-tuning is completed and the model is saved in './results'
from transformers import T5ForConditionalGeneration, T5Tokenizer
import torch

# Load the fine-tuned model and tokenizer
model = T5ForConditionalGeneration.from_pretrained('./results/checkpoint-1752')  # Path to your fine-tuned model
tokenizer = T5Tokenizer.from_pretrained('t5-base')  # Path to your tokenizer

# Move the model to the GPU if available
device = "cuda" if torch.cuda.is_available() else "cpu"
model.to(device)

# Provide a new project description for risk prediction
new_project_description = """Global
The InfoShop
To make information and resources on development topics more readily available to stakeholders around the world.
0
2002
The World Bank has a wealth of information and resources that can be used to support development efforts around the world. However, this information is often scattered and difficult to access. The InfoShop provides a central repository for this information, making it easier for stakeholders to find ...
The World Bank is a global institution that works to reduce poverty and improve living standards around the world. The World Bank has a wealth of information and resources that can be used to support development efforts. The InfoShop is a Web site that provides access to this information and resourc...
The goal of the InfoShop is to make information and resources on development topics more readily available to stakeholders around the world. The InfoShop will also help to promote the World Bank's work and build its reputation as a leading source of development information and knowledge.
The InfoShop is a Web site that provides access to information and resources on a wide range of development topics, including poverty reduction, education, health, and climate change. The InfoShop is designed to be user-friendly and accessible to a wide range of stakeholders. The site includes a var...
The InfoShop is a valuable opportunity to make the World Bank's information and resources more readily available to stakeholders around the world. The InfoShop can help to promote the World Bank's work and build its reputation as a leading source of development information and knowledge.
The InfoShop is subject to the constraints of the World Bank's overall budget and resources. The InfoShop will need to be updated regularly to keep its content current. The InfoShop will need to be promoted effectively to reach its target audience. The InfoShop will need to be maintained securely to...
The InfoShop will be accessible 24 hours a day, seven days a week. The World Bank will provide ongoing support to the  """

# Prepare the input for the model
input_text = "generate a list of risks: " + new_project_description
input_ids = tokenizer(input_text, return_tensors="pt").input_ids.to(device)

# Generate the output (risk assessment)
outputs = model.generate(input_ids, max_length=100, num_beams=4, early_stopping=True)

# Decode the generated output
generated_risk = tokenizer.decode(outputs[0], skip_special_tokens=True)
print(f"Risk Assessment: {generated_risk}")
