"""
This script provides the AmdOcrChecker class, which matches PDF filenames to account and contract information using account_files.json and filename_to_account.json.
If a match is found (and the contract start date condition is met), it extracts the full PDF content and transforms it into structured markdown using OpenAI.

Inputs:
- account_files.json: Maps account IDs to files and start dates (and optionally account names).
- filename_to_account.json: Maps filenames to account IDs, contract IDs, and start dates.
- PDF file: The PDF to be checked and processed.

Return values:
- If matched: (markdown_content, contractid, account_id, start_date, matched_start_date, accountname)
- If not matched: ("", None, account_id, accountname) or ("", None, None, None) if no account is found.

Environment:
- Requires OPENAI_API_KEY in a .env file for markdown extraction.
"""
import json
import re
from datetime import datetime
import pymupdf
from openai import AsyncOpenAI
import asyncio
from config import get_ssm_param


class AmdOcrChecker:
    system_prompt = """Analyze the text in the provided text. Extract all readable content
and present it in a structured Markdown format that is clear, concise,
and well-organized. Ensure proper formatting (e.g., headings, lists, or
code blocks) as necessary to represent the content effectively. Ensure
verbatim extraction.

Do not include the Appendix sections in the extraction. Basically, extract all except for the appendix sections.
"""

    def __init__(self):
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        self.async_openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)

    def _load_json(self, path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def normalize_filename(self, name: str) -> str:
        """Remove all extensions and normalize for comparison."""
        return re.sub(r'(\.[a-zA-Z0-9]+)+$', '', name).strip().lower()

    async def extract_markdown_from_pdf(self, pdf_content):
        try:
            print("Extracting text from PDF...")
            doc = pymupdf.open(stream=pdf_content)
            
            # Extract text from all pages
            full_text = ""
            for page_num in range(len(doc)):
                page = doc.load_page(page_num)
                full_text += page.get_text()
            
            doc.close()  # Don't forget to close the document
            
            if not full_text.strip():  # Now check the actual text
                print("No text extracted from PDF.")
                return "No text extracted from PDF."
                
            print("Text extracted. Sending to OpenAI for markdown conversion...")

            # Add delay before API call
            await asyncio.sleep(5)            
            response = await self.async_openai.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "user", "content": self.system_prompt},
                    {"role": "user", "content": full_text}
                ],
            )
            print("Received response from OpenAI.")
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error during markdown extraction: {e}")
            return f"Error during markdown extraction: {e}"

    async def check(self, filename, pdf_content, account_files, filename_to_account):
        norm_filename = self.normalize_filename(filename)
        for account_id, account_info in account_files.items():
            for fname, start_date in account_info.get('Files', {}).items():
                if self.normalize_filename(fname) == norm_filename:
                    matched_start_date = None
                    contractid = None
                    for entry in filename_to_account.values():
                        if entry.get('AccountId') == account_id:
                            matched_start_date = entry.get('StartDate')
                            contractid = entry.get('Contractid')
                            break
                    accountname = account_info.get('AccountName')
                    if matched_start_date and start_date:
                        try:
                            dt1 = datetime.strptime(matched_start_date, "%Y-%m-%d")
                            dt2 = datetime.strptime(start_date, "%Y-%m-%d")
                            if dt1 < dt2:
                                print("Match found. Proceeding to extract markdown from PDF...")
                                markdown_content = await self.extract_markdown_from_pdf(pdf_content)
                                print("Markdown extraction complete.")
                                return (markdown_content, contractid, account_id, start_date, matched_start_date, accountname)
                            else:
                                return ("unmatched", "no_external_id", account_id, accountname, None, None)
                        except Exception:
                            return ("unmatched", "no_external_id", account_id, accountname, None, None)
                    return ("unmatched", "no_external_id", account_id, accountname, None, None)
        return ("unmatched", "no_external_id", None, None, None, None)

# if __name__ == "__main__":
#     # Hardcoded input paths
#     account_files_path = "account_files.json"
#     filename_to_account_path = "filename_to_account.json"
#     extracted_data_path = "BACKEND/THIRDV/amendments/extracted_data_updated.json"
#     pdf_path = "BACKEND/THIRDV/amendments/Trust Payments - Credit UK DS add on.pdf"

#     checker = AmdOcrChecker(account_files_path, filename_to_account_path, extracted_data_path)
#     result = asyncio.run(checker.check(pdf_path))
#     print(result)