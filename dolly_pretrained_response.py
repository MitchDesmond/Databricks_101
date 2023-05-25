# Databricks notebook source
# MAGIC %md
# MAGIC # Intro
# MAGIC The purpose of this notebook is to simply test the dolly model **without training**, the model is available on Hugging Face as [databricks/dolly-v2-12b](https://huggingface.co/databricks/dolly-v2-12b).  The purpose to dolly is to create similar outputs to ChatGPT with a much lower cost.  For more information please see the [Original Blog Post](https://www.databricks.com/blog/2023/03/24/hello-dolly-democratizing-magic-chatgpt-open-models.html).
# MAGIC
# MAGIC If you would like to learn how to train dolly for your own use case and corpora please see the [git page](https://github.com/databrickslabs/dolly).
# MAGIC
# MAGIC You can also watch a [youtube demo](https://www.youtube.com/watch?v=Xp0sAghk28M&ab_channel=AdvancingAnalytics) of the below code.
# MAGIC
# MAGIC [Basic info on accelerate](https://huggingface.co/blog/accelerate-large-models)

# COMMAND ----------

# MAGIC %md
# MAGIC # Install packages

# COMMAND ----------

# MAGIC %pip install accelerate
# MAGIC # #may need line below
# MAGIC %pip install transformers

# COMMAND ----------

# MAGIC %md
# MAGIC # Load Model & Tokenizer

# COMMAND ----------

import numpy as np
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    PreTrainedModel,
    PreTrainedTokenizer
)

#tokenizer = AutoTokenizer.from_pretrained("databricks/dolly-v2-12b", padding_side="left")
#model = AutoModelForCausalLM.from_pretrained("databricks/dolly-v2-12b", device_map="auto", trust_remote_code=True, offload_folder="offload")
tokenizer = AutoTokenizer.from_pretrained("databricks/dolly-v2-7b", padding_side="left")
model = AutoModelForCausalLM.from_pretrained("databricks/dolly-v2-7b", device_map="auto", trust_remote_code=True, offload_folder="offload")

# COMMAND ----------

# MAGIC %md
# MAGIC # Generate text

# COMMAND ----------

PROMPT_FORMAT = """Below is an instruction that describes a task. Write a response that appropriately completes the request.

### Instruction:
{instruction}

### Response:
"""

def generate_response(instruction: str, *, model: PreTrainedModel, tokenizer: PreTrainedTokenizer, 
                      do_sample: bool = True, max_new_tokens: int = 256, top_p: float = 0.92, top_k: int = 0, **kwargs) -> str:
    input_ids = tokenizer(PROMPT_FORMAT.format(instruction=instruction), return_tensors="pt").input_ids.to("cuda")

    # each of these is encoded to a single token
    response_key_token_id = tokenizer.encode("### Response:")[0]
    end_key_token_id = tokenizer.encode("### End")[0]

    gen_tokens = model.generate(input_ids, pad_token_id=tokenizer.pad_token_id, eos_token_id=end_key_token_id,
                                do_sample=do_sample, max_new_tokens=max_new_tokens, top_p=top_p, top_k=top_k, **kwargs)[0].cpu()

    # find where the response begins
    response_positions = np.where(gen_tokens == response_key_token_id)[0]

    if len(response_positions) >= 0:
        response_pos = response_positions[0]
        
        # find where the response ends
        end_pos = None
        end_positions = np.where(gen_tokens == end_key_token_id)[0]
        if len(end_positions) > 0:
            end_pos = end_positions[0]

        return tokenizer.decode(gen_tokens[response_pos + 1 : end_pos]).strip()

    return None


# COMMAND ----------

# MAGIC %md
# MAGIC # test output

# COMMAND ----------

# Sample similar to: "Excited to announce the release of Dolly, a powerful new language model from Databricks! #AI #Databricks"
generate_response("Write a tweet announcing Dolly, a large language model from Databricks.", model=model, tokenizer=tokenizer)


# COMMAND ----------

generate_response("Is a hotdog a sandwich?", model=model, tokenizer=tokenizer)

# COMMAND ----------

generate_response("explain the python concept of __init__ in simple terms", model=model, tokenizer=tokenizer)

# COMMAND ----------


