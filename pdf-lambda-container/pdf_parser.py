import base64
import re
import io
import fitz
from PIL import Image
from openai import AsyncOpenAI
import logging
from pdfminer.high_level import extract_text
from config import get_ssm_param
import asyncio

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

    def encode_image(self, image_bytes):
        """Encode image bytes to base64"""
        return base64.b64encode(image_bytes).decode("utf-8")

    async def ocr_openai(self, image_bytes):
        base64_image = self.encode_image(image_bytes)
        client = AsyncOpenAI(api_key=self.OPENAI_API_KEY)
        try:
            # Add delay before API call
            await asyncio.sleep(5)
            response = await client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": self.PARSER_PROMPT,
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
        
    def extract_appendix_locs(self, pages):
        pattern = re.compile(r'appendix(?: 1)?: business verification country groupings', re.IGNORECASE)
        start_index = next((i for i, item in enumerate(pages) if pattern.search(item)), None)
        result = list(range(start_index, len(pages))) if start_index is not None else []
        return result

    async def parse_pdf(self, pdf_content) -> str:
        logger.info("Starting PDF parsing from S3 content")
        
        # Extract text first
        text = extract_text(io.BytesIO(pdf_content))
        pages = text.split('\x0c')
        pages = pages[:-1]  # Remove last blank page

        appendix_indexes = self.extract_appendix_locs(pages)
        appendix_indexes.insert(0, 0)  # Always keep the first page

        try:
            # Open PDF document from bytes
            doc = fitz.open(stream=pdf_content, filetype="pdf")
            total_pages = len(doc)
            
            logger.info(f"PDF has {total_pages} pages. Converting to images...")

            images = []

            # Process each page
            for page_num in range(total_pages):          
                page = doc[page_num]
                
                dpi = 150
                mat = fitz.Matrix(dpi/72, dpi/72)
                pix = page.get_pixmap(matrix=mat)
                
                # Convert to PIL Image
                img_data = pix.tobytes("ppm")
                pil_image = Image.open(io.BytesIO(img_data))
                
                # Convert to JPEG format
                jpeg_buffer = io.BytesIO()
                pil_image.save(jpeg_buffer, format='JPEG')
                jpeg_buffer.seek(0)
                
                images.append(jpeg_buffer.getvalue())

            filtered_images = [img for i, img in enumerate(images) if i not in set(appendix_indexes)]

            markdowns = []
            for i, img_bytes in enumerate(filtered_images):
                logger.info(f"Processing page {i+2}...")
                extract_page_content = await self.ocr_openai(img_bytes)
                markdowns.append(extract_page_content)

            doc.close()

            return "\n\n".join(markdowns)

        except Exception as e:
            logger.error(f"Error processing PDF: {str(e)}")
            return f"Error processing PDF: {str(e)}"