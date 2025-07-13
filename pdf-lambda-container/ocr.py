import re
import base64
from openai import AsyncOpenAI
import logging
from pdfminer.high_level import extract_text
from pdf2image import convert_from_path
from config import get_ssm_param
import asyncio
from io import BytesIO
import io

# Configure logging
logger = logging.getLogger(__name__)

class PDFParser:
    def __init__(self):
        self.OPENAI_API_KEY = get_ssm_param("/myapp/openai_api_key")
        # self.PARSER_PROMPT = get_ssm_param("/myapp/parser_prompt")
        self.PARSER_PROMPT = """Analyze the text in the provided image. Extract all readable content
                        and present it in a structured Markdown format that is clear, concise,
                        and well-organized. Ensure proper formatting (e.g., headings, lists, or
                        code blocks) as necessary to represent the content effectively. Ensure
                        verbatim extraction.

                        Do not include the Appendix sections in the extraction. Basically, extract all except for the appendix sections.
                        """
        self.openai = AsyncOpenAI(api_key=self.OPENAI_API_KEY)
        
    async def ocr_openai(self, image):
        buffer = BytesIO()
        image.save(buffer, format='JPEG')
        buffer.seek(0)
        encoded_image = base64.b64encode(buffer.read()).decode('ascii')

        try:
            # Add delay before API call
            await asyncio.sleep(1)

            response = await self.openai.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": self.PARSER_PROMPT},
                            {"type": "image_url", "image_url": {"url": f"data:image/jpeg;base64,{encoded_image}"}},
                        ],
                    }
                ],
            )
            return response.choices[0].message.content
        except Exception as e:
            return (f"Error processing image: {str(e)}")
        
    def extract_appendix_locs(self, pages):
        pattern = re.compile(r'appendix 1: business verification country groupings', re.IGNORECASE)
        start_index = next((i for i, item in enumerate(pages) if pattern.search(item)), None)
        result = list(range(start_index, len(pages))) if start_index is not None else []
        return result

    async def parse_pdf(self, pdf_content):
        text = extract_text(io.BytesIO(pdf_content))
        pages = text.split('\x0c')
        pages = pages[:-1]  # Remove last blank page

        appendix_indexes = self.extract_appendix_locs(pages)
        appendix_indexes.insert(0, 0)  # Always keep the first page

        images = convert_from_path(pdf_content)
        filtered_images = [img for i, img in enumerate(images) if i not in set(appendix_indexes)]

        markdowns = []
        for i, img in enumerate(filtered_images):
            print(f"Processing page {i+2}...")
            extract_page_content = await self.ocr_openai(img)
            markdowns.append(extract_page_content)

        return "\n\n".join(markdowns)