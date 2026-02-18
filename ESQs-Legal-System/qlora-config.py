#!/usr/bin/env python3
"""
LAW Matrix v4.0 - QLoRA Fine-Tuning Configuration
Multi-layered fine-tuning strategy for observant and intelligent legal AI
"""

import torch
from transformers import (
    AutoTokenizer, 
    AutoModelForCausalLM, 
    BitsAndBytesConfig,
    TrainingArguments,
    Trainer,
    DataCollatorForLanguageModeling
)
from peft import LoraConfig, get_peft_model, TaskType, PeftModel
from datasets import Dataset
import json
import os
from typing import Dict, List, Any

class LawMatrixQLoRAConfig:
    """
    QLoRA configuration for LAW Matrix v4.0 Bulletproof Enterprise Edition
    Implements memory-efficient adaptation for legal AI specialization
    """
    
    def __init__(self):
        # Base model configuration - using Llama-2-7B for legal reasoning
        self.base_model_name = "meta-llama/Llama-2-7b-chat-hf"
        
        # QLoRA Configuration
        self.lora_config = LoraConfig(
            task_type=TaskType.CAUSAL_LM,
            inference_mode=False,
            r=16,  # Rank of adaptation
            lora_alpha=32,  # LoRA scaling parameter
            lora_dropout=0.1,
            target_modules=["q_proj", "v_proj", "k_proj", "o_proj", "gate_proj", "up_proj", "down_proj"]
        )
        
        # 4-bit quantization configuration
        self.quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_use_double_quant=True,
            bnb_4bit_quant_type="nf4",
            bnb_4bit_compute_dtype=torch.bfloat16
        )
        
        # Training arguments
        self.training_args = TrainingArguments(
            output_dir="./lawmatrix-lora-adapters",
            num_train_epochs=3,
            per_device_train_batch_size=1,
            per_device_eval_batch_size=1,
            gradient_accumulation_steps=4,
            warmup_steps=100,
            learning_rate=2e-4,
            fp16=True,
            logging_steps=10,
            save_steps=500,
            eval_steps=500,
            evaluation_strategy="steps",
            save_strategy="steps",
            load_best_model_at_end=True,
            report_to="none",  # Disable wandb for now
            remove_unused_columns=False,
        )

class LawMatrixDataProcessor:
    """
    Processes legal training data for QLoRA fine-tuning
    Focuses on legal reasoning, case analysis, and document generation
    """
    
    def __init__(self, tokenizer):
        self.tokenizer = tokenizer
        self.max_length = 2048
        
    def create_legal_instruction_dataset(self) -> Dataset:
        """
        Creates instruction-following dataset for legal AI training
        Includes case analysis, document generation, and legal reasoning
        """
        
        legal_instructions = [
            {
                "instruction": "Analyze the legal issues in the following case: Stears v. Stears, Utah family law case involving custody and property division.",
                "input": "Case involves parties married 15 years, two minor children, disputed custody arrangement, and significant marital property including business assets.",
                "output": "Based on Utah Code Title 30 and Utah Rules of Civil Procedure, this case involves three primary legal issues:\n1. Child custody determination under Utah Code § 30-3-10\n2. Marital property division under Utah Code § 30-3-5\n3. Spousal support consideration under Utah Code § 30-3-5\n\nKey factors for custody: child's best interests, parental fitness, child's preference if age 14+\nKey factors for property: equitable distribution, contribution to acquisition, economic circumstances\nRecommended legal strategy: focus on child's best interests while protecting client's economic position."
            },
            {
                "instruction": "Generate a legal memorandum analyzing potential malpractice risks in client representation.",
                "input": "Client seeks representation in complex family law matter with multiple jurisdictions involved.",
                "output": "MALPRACTICE PREVENTION ANALYSIS:\n\n1. Jurisdictional Issues:\n   - Verify proper venue under Utah Rules of Civil Procedure 4\n   - Confirm subject matter jurisdiction for family law matters\n   - Check for parallel proceedings in other states\n\n2. Client Communication:\n   - Document all client meetings and communications\n   - Provide written fee agreements and scope of representation\n   - Regular case status updates to prevent abandonment claims\n\n3. Legal Research:\n   - Use LexisNexis for current law verification\n   - Check for recent appellate decisions affecting case\n   - Verify statute of limitations and filing deadlines\n\n4. Ethical Considerations:\n   - Avoid conflicts of interest per Utah Rules of Professional Conduct\n   - Maintain client confidentiality\n   - Competent representation standard compliance"
            },
            {
                "instruction": "Draft a motion for summary judgment in a family law case.",
                "input": "Motion for summary judgment regarding undisputed facts in custody determination case.",
                "output": "IN THE DISTRICT COURT OF UTAH COUNTY, UTAH\n\nJANE DOE, )\nPlaintiff, )\nvs. )\nJOHN DOE, )\nDefendant. )\n\nMOTION FOR SUMMARY JUDGMENT\n\nComes now Plaintiff, by and through counsel, and respectfully moves this Court for summary judgment pursuant to Utah Rule of Civil Procedure 56, on the grounds that there are no genuine issues of material fact and Plaintiff is entitled to judgment as a matter of law.\n\nSTATEMENT OF UNDISPUTED FACTS:\n1. The parties were married on [DATE]\n2. Two minor children were born of the marriage\n3. Defendant has been convicted of domestic violence\n4. Defendant has not exercised visitation for 12 months\n\nARGUMENT:\nUnder Utah Code § 30-3-10, the Court must consider the best interests of the child. The undisputed facts demonstrate that Defendant poses a risk to the children's safety and welfare.\n\nWHEREFORE, Plaintiff respectfully requests this Court grant summary judgment in favor of Plaintiff regarding custody determination."
            }
        ]
        
        # Format for instruction tuning
        formatted_data = []
        for item in legal_instructions:
            prompt = f"<s>[INST] {item['instruction']}\n\n{item['input']} [/INST] {item['output']} </s>"
            formatted_data.append({"text": prompt})
            
        return Dataset.from_list(formatted_data)
    
    def tokenize_dataset(self, dataset: Dataset) -> Dataset:
        """Tokenizes the dataset for training"""
        
        def tokenize_function(examples):
            return self.tokenizer(
                examples["text"],
                truncation=True,
                padding=False,
                max_length=self.max_length,
                return_tensors="pt"
            )
            
        return dataset.map(tokenize_function, batched=True, remove_columns=dataset.column_names)

class LawMatrixFineTuner:
    """
    Main fine-tuning orchestrator for LAW Matrix v4.0
    Implements the complete QLoRA training pipeline
    """
    
    def __init__(self):
        self.config = LawMatrixQLoRAConfig()
        self.tokenizer = None
        self.model = None
        self.peft_model = None
        
    def setup_model_and_tokenizer(self):
        """Initialize model and tokenizer with QLoRA configuration"""
        
        print("🚀 LAW Matrix v4.0 - Initializing QLoRA Fine-Tuning...")
        
        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.config.base_model_name,
            trust_remote_code=True,
            padding_side="right"
        )
        self.tokenizer.pad_token = self.tokenizer.eos_token
        
        # Load model with quantization
        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.base_model_name,
            quantization_config=self.config.quantization_config,
            device_map="auto",
            trust_remote_code=True
        )
        
        # Apply LoRA
        self.peft_model = get_peft_model(self.model, self.config.lora_config)
        self.peft_model.print_trainable_parameters()
        
        print("✅ LAW Matrix v4.0 - QLoRA setup complete")
        
    def train_model(self):
        """Execute the fine-tuning process"""
        
        print("🧠 LAW Matrix v4.0 - Starting fine-tuning process...")
        
        # Create and process dataset
        processor = LawMatrixDataProcessor(self.tokenizer)
        dataset = processor.create_legal_instruction_dataset()
        tokenized_dataset = processor.tokenize_dataset(dataset)
        
        # Data collator
        data_collator = DataCollatorForLanguageModeling(
            tokenizer=self.tokenizer,
            mlm=False
        )
        
        # Initialize trainer
        trainer = Trainer(
            model=self.peft_model,
            args=self.config.training_args,
            train_dataset=tokenized_dataset,
            data_collator=data_collator,
        )
        
        # Train
        trainer.train()
        
        # Save the adapter
        trainer.model.save_pretrained("./lawmatrix-lora-adapters")
        self.tokenizer.save_pretrained("./lawmatrix-lora-adapters")
        
        print("✅ LAW Matrix v4.0 - Fine-tuning complete!")
        
    def load_fine_tuned_model(self, adapter_path: str):
        """Load the fine-tuned model for inference"""
        
        # Load base model
        self.model = AutoModelForCausalLM.from_pretrained(
            self.config.base_model_name,
            quantization_config=self.config.quantization_config,
            device_map="auto",
            trust_remote_code=True
        )
        
        # Load LoRA adapter
        self.peft_model = PeftModel.from_pretrained(self.model, adapter_path)
        
        # Load tokenizer
        self.tokenizer = AutoTokenizer.from_pretrained(adapter_path)
        
        print("✅ LAW Matrix v4.0 - Fine-tuned model loaded!")

if __name__ == "__main__":
    # Initialize and run fine-tuning
    fine_tuner = LawMatrixFineTuner()
    fine_tuner.setup_model_and_tokenizer()
    fine_tuner.train_model()
