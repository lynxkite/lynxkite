import os
import openai
import cmd

openai.api_key = os.getenv("OPENAI_API_KEY")
openai.api_key_path = "API_KEY"

class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'

def generateSQL(query: str, inputSchema: str, outputSchema: str):
    prompt:str = "### SQLite SQL tables, with their properties:\n" + \
    "# " + inputSchema + \
    "\n#\n### " + query + \
    "\n### Please provide the results in the following format: " + outputSchema + \
    "\n\nSELECT"
    response = openai.Completion.create(
                model="text-davinci-003",
                prompt=prompt,
                temperature=0,
                max_tokens=1000,
                top_p=1,
                frequency_penalty=0.0,
                presence_penalty=0.0,
                stop=["#", "Result"]
            )
    print(color.DARKCYAN + prompt + "\n\n" + color.END)
    return "SELECT" + response['choices'][0]['text']

print(generateSQL(input("query: "), input("inputSchema: "), input("outputSchema: ")))
