#!/usr/bin/env python3
"""
OCR Check - Amendment Processing Pipeline

PURPOSE:
This script handles the OCR stage of amendment processing by extracting customer names
from PDFs and determining if they match existing contracts in the JSON data.

FLOW:
1. INPUT: PDF file + JSON file (containing existing contracts)
2. PROCESS:
   - Convert PDF pages to images using PIL
   - Process first 2 pages to extract customer name using LLM
   - Compare customer name with existing contracts in JSON
   - If MATCHED: Process entire document and return markdown
   - If UNMATCHED: Return empty string (no processing)
3. OUTPUT:
   - MATCHED: (markdown_content, contract_external_id, customer_name)
   - UNMATCHED: ("", None, customer_name)

USAGE:
- Standalone: python ocr_check.py
- As module: await parser.parse_pdf(pdf_path, json_data)
"""

import io
import base64
import asyncio
from openai import AsyncOpenAI
from PIL import Image
import pymupdf
print(f"DEBUG: PyMuPDF version: {pymupdf.__version__}")
from config import get_ssm_param

system_prompt = """Analyze the text in the provided image. Extract all readable content
                and present it in a structured Markdown format that is clear, concise,
                and well-organized. Ensure proper formatting (e.g., headings, lists, or
                code blocks) as necessary to represent the content effectively. Ensure
                verbatim extraction.

                Do not include the Appendix sections in the extraction. Basically, extract all except for the appendix sections.
                """

class PDFParser:
    def __init__(self):
        api_key = get_ssm_param("/myapp/openai_api_key")
        self.async_openai = AsyncOpenAI(api_key=api_key)
        
        # Parser prompt for customer name extraction
        self.parser_prompt = (
            "You are an intelligent document parser. Your task is to extract only the Customer Name from the provided document text.\n"
            "The name of the Customer, always before the (\"Customer\") string and the first line under Customer Information.\n"
            "Do not output markdown or any other content. Only return the Customer Name as plain text.\n"
            "\n"
            "For example, if the document contains:\n"
            "'AmWager (\"Customer\")'\n"
            "Your output should be:\n"
            "AmWager\n"
            "(Do not include any label, such as 'Customer Name:' or 'AccountName:'. Only output the name itself.)"
        )

    def encode_image(self, image_bytes):
        """Encode image bytes to base64"""
        return base64.b64encode(image_bytes).decode("utf-8")

    async def extract_customer_name(self, markdown_text):
        """Extract customer name from markdown text using LLM."""
        try:
            # Add delay before API call
            await asyncio.sleep(5)

            response = await self.async_openai.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "user", "content": self.parser_prompt},
                    {"role": "user", "content": markdown_text}
                ],
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"Error extracting customer name: {str(e)}"

    async def process_text(self, image_bytes):
        """Process image content with OpenAI."""
        try:
            # Add delay before API call
            await asyncio.sleep(5)

            base64_image = self.encode_image(image_bytes)
            response = await self.async_openai.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": system_prompt,
                            },
                            {
                                "type": "image_url",
                                "image_url": {"url": f"data:image/jpeg;base64,{base64_image}"},
                            },
                        ],
                    }
                ],
            )
            return response.choices[0].message.content
        except Exception as e:
            return f"Error processing image: {str(e)}"

    def get_contracts_from_json(self, json_data):
        """Extract contracts from various JSON formats."""
        if "Contract" in json_data:
            contracts = json_data["Contract"]
            return contracts if isinstance(contracts, list) else contracts.get("data", [])
        else:
            # Handle output_records format
            for record in json_data.get("output_records", []):
                if record.get("name") == "Contract":
                    return record.get("data", [])
        return []

    def normalize(self, s):
        """Normalize string for comparison."""
        return (s or "").replace(" ", "").lower()

    def get_filtered_pages(self, images):
        """Get images excluding appendix sections."""
        # For now, return all images since we can't easily detect appendix in images
        # This can be enhanced later if needed
        return images

    async def parse_pdf(self, pdf_content, json_data=None):
        """PDF parser that extracts customer name and processes document if match found."""
        try:
            # Open PDF document
            doc = pymupdf.open(stream=pdf_content)

            total_pages = len(doc)
            
            if total_pages == 0:
                print("No pages to process.")
                return "unmatched", "no_external_id", ""

            # Convert first 2 pages to images for customer name extraction
            print("Processing first 2 pages for customer name extraction...")
            initial_images = []
            for page_num in range(min(2, total_pages)):
                page = doc[page_num]
                dpi = 150
                mat = pymupdf.Matrix(dpi/72, dpi/72)
                pix = page.get_pixmap(matrix=mat)
                
                # Convert to PIL Image
                img_data = pix.tobytes("ppm")
                pil_image = Image.open(io.BytesIO(img_data))
                
                # Convert to JPEG format
                jpeg_buffer = io.BytesIO()
                pil_image.save(jpeg_buffer, format='JPEG')
                jpeg_buffer.seek(0)
                
                initial_images.append(jpeg_buffer.getvalue())

            # Process first 2 pages for customer name extraction
            initial_markdowns = [await self.process_text(img_bytes) for img_bytes in initial_images]
            initial_markdown = "\n\n".join(initial_markdowns)
            customer_name = await self.extract_customer_name(initial_markdown)
            print(f"Extracted customer name: {customer_name}")

            # If no JSON data, return early
            if not json_data:
                doc.close()
                return "unmatched", "no_external_id", customer_name

            # Extract contracts and check for match
            contracts = self.get_contracts_from_json(json_data)
            account_names = [c.get("AccountName") for c in contracts if c.get("AccountName")]
            contract_ids = [c.get("ContractExternalId") for c in contracts if c.get("AccountName")]
            
            norm_customer = self.normalize(customer_name)
            norm_accounts = [self.normalize(name) for name in account_names]
            
            print(f"Normalized customer: {norm_customer}")
            print(f"Normalized accounts: {norm_accounts}")
            
            # Check for match and process accordingly
            if norm_customer in norm_accounts:
                print("✅ Match found! Processing entire document...")
                match_index = norm_accounts.index(norm_customer)
                contract_external_id = contract_ids[match_index]
                
                # Convert all pages to images
                images = []
                for page_num in range(total_pages):
                    page = doc[page_num]
                    dpi = 150
                    mat = pymupdf.Matrix(dpi/72, dpi/72)
                    pix = page.get_pixmap(matrix=mat)
                    
                    # Convert to PIL Image
                    img_data = pix.tobytes("ppm")
                    pil_image = Image.open(io.BytesIO(img_data))
                    
                    # Convert to JPEG format
                    jpeg_buffer = io.BytesIO()
                    pil_image.save(jpeg_buffer, format='JPEG')
                    jpeg_buffer.seek(0)
                    
                    images.append(jpeg_buffer.getvalue())

                # Filter out appendix pages
                filtered_images = self.get_filtered_pages(images)
                markdowns = [await self.process_text(img_bytes) for img_bytes in filtered_images]
                
                doc.close()
                return "\n\n".join(markdowns), contract_external_id, customer_name
            else:
                print("❌ No match found.")
                doc.close()
                return "unmatched", "no_external_id", customer_name
                
        except Exception as e:
            print(f"Error processing PDF: {str(e)}")
            return "unmatched", "no_external_id", ""

if __name__ == "__main__":
    # Configuration
    input_pdf_path = "Plum_New_Markets_add_ons.docx.pdf"
    input_json_path = "extracted_data.json"
    output_folder = "output_json"
    
    os.makedirs(output_folder, exist_ok=True)

    # Load JSON and process PDF
    with open(input_json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)

    parser = PDFParser()
    output_markdown, contract_external_id, customer_name = asyncio.run(
        parser.parse_pdf(input_pdf_path, json_data)
    )

    # Save results
    base_filename = os.path.splitext(os.path.basename(input_pdf_path))[0]
    
    # Save markdown content
    if output_markdown == "unmatched":
        markdown_path = os.path.join(output_folder, f"{base_filename}_unmatched.md")
        with open(markdown_path, "w", encoding="utf-8") as f:
            f.write("unmatched")
        print(f"\nUnmatched markdown saved to {markdown_path}")
    else:
        markdown_path = os.path.join(output_folder, f"parsed_{base_filename}.md")
        with open(markdown_path, "w", encoding="utf-8") as f:
            f.write(output_markdown)
        print(f"\nMarkdown extracted and saved to {markdown_path}")
    
    # Save external ID
    external_id_path = os.path.join(output_folder, f"{base_filename}_external_id.txt")
    with open(external_id_path, "w", encoding="utf-8") as f:
        f.write(str(contract_external_id))
    print(f"External ID saved to {external_id_path}")
    
    # Save customer name
    customer_name_path = os.path.join(output_folder, f"{base_filename}_customer_name.txt")
    with open(customer_name_path, "w", encoding="utf-8") as f:
        f.write(str(customer_name))
    print(f"Customer name saved to {customer_name_path}")

    # Print results
    print(f"\n========== EXTRACTION RESULTS ==========")
    print(f"Customer Name: {customer_name}")
    print(f"Contract External ID: {contract_external_id}")
    print(f"Markdown Status: {'Unmatched' if output_markdown == 'unmatched' else 'Extracted'}")
    
    if output_markdown != "unmatched":
        print(f"\n========== MARKDOWN CONTENT ==========")
        print(output_markdown)
        print("========== MARKDOWN CONTENT END ==========")
    print("\n========== EXTRACTION RESULTS END ==========")