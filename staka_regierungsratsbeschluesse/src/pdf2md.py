import os
import base64
from pathlib import Path
import tempfile
import markdown
import fitz  # PyMuPDF
import pypandoc
import re
from weasyprint import HTML, CSS
from weasyprint.text.fonts import FontConfiguration
import importlib.util
import shutil
from pdf2image import convert_from_path
import subprocess
import os
import io
import pymupdf4llm
from docling.document_converter import DocumentConverter
import openai
import pdfplumber
from mistralai import Mistral
from PIL import Image
import zipfile

IMAGE_FOLDER = Path("./images")
if not IMAGE_FOLDER.exists():
    IMAGE_FOLDER.mkdir()

class Converter():
    def __init__(self, lib: str, input_file: Path):
        self.lib = lib
        self.input_file = input_file
        fd, temp_path = tempfile.mkstemp(suffix=".md")
        self.output_file = Path(temp_path)  # Store as Path object for easy handling
        self.doc_image_folder = Path(f'{IMAGE_FOLDER}/{self.output_file.stem}')
        self.doc_image_folder.mkdir(parents=True, exist_ok=True)
        self.md_content = ""
        self.create_image_zip_file = False
    
    def has_image_extraction(self):
        return self.lib.lower() in ['mistral-ocr']
    
    def extract_images_from_pdf(self):
        pdf_document = fitz.open(self.input_file)
        img_index=0
        # Iterate through the pages
        for page_num in range(len(pdf_document)):
            page = pdf_document.load_page(page_num)
            images = page.get_images(full=True)
            # Iterate through the images on the page
            for i, img in enumerate(images):
                try:
                    xref = img[0]
                    base_image = pdf_document.extract_image(xref)
                    image_bytes = base_image["image"]
                    image = Image.open(io.BytesIO(image_bytes))
                    # Save the image
                    image_path = self.doc_image_folder / f'img_{img_index}.png'
                    print(image_path.name)
                    image.save(image_path)
                except Exception as e:
                    print(f"Error extracting image: {str(e)}")    
                img_index += 1


    def pymupdf_conversion(self):
        """Convert PDF to markdown using PyMuPDF (fitz) for text extraction and custom formatting"""
        doc = fitz.open(self.input_file)
        text_blocks = []
        for page_num in range(len(doc)):
            page = doc[page_num]
            # Get text blocks with formatting information
            blocks = page.get_text("dict")["blocks"]
            
            for block in blocks:
                if "lines" in block:
                    for line in block["lines"]:
                        if "spans" in line:
                            line_text = ""
                            is_bold = False
                            is_heading = False
                            font_size = 0
                            
                            for span in line["spans"]:
                                # Check for formatting hints
                                if span["text"].strip():
                                    current_font_size = span["size"]
                                    current_font = span["font"].lower()
                                    current_text = span["text"]
                                    
                                    # Detect possible headings based on font size
                                    if current_font_size > font_size:
                                        font_size = current_font_size
                                    
                                    # Detect bold text
                                    if "bold" in current_font or span["flags"] & 2:  # 2 is bold flag
                                        is_bold = True
                                    
                                    line_text += current_text
                            
                            if line_text.strip():
                                # Determine if this might be a heading based on font size
                                if font_size > 12:  # Arbitrary threshold - adjust as needed
                                    is_heading = True
                                
                                text_blocks.append({
                                    "text": line_text.strip(),
                                    "is_bold": is_bold,
                                    "is_heading": is_heading,
                                    "font_size": font_size,
                                    "page": page_num + 1
                                })
        
        # Convert to markdown
        md_lines = []
        prev_block = None
        
        for block in text_blocks:
            text = block["text"].strip()
            # Skip empty lines
            if not text:
                continue
                
            # Detect headings based on formatting and content
            if block["is_heading"] or (len(text) < 80 and not text.endswith(('.', ',', ';', ':', '?', '!'))):
                # Determine heading level based on font size
                if block["font_size"] >= 18:
                    md_lines.append(f"# {text}")
                elif block["font_size"] >= 16:
                    md_lines.append(f"## {text}")
                elif block["font_size"] >= 14:
                    md_lines.append(f"### {text}")
                elif block["is_bold"]:
                    md_lines.append(f"**{text}**")
                else:
                    md_lines.append(text)
            else:
                # Regular text paragraph
                if block["is_bold"]:
                    md_lines.append(f"**{text}**")
                else:
                    md_lines.append(text)
        
            # Add separator between blocks from different pages
            if prev_block and prev_block["page"] != block["page"]:
                md_lines.append("\n---\n")
            prev_block = block
    
        # Join all lines
        md_content = "\n\n".join(md_lines)
        md_content = re.sub(r'\n{3,}', '\n\n', md_content)
        return md_content

    def pymupdf4llm_conversion(self):
        """Convert PDF to markdown using pymupdf4llm"""
        try:
            md_content = pymupdf4llm.to_markdown(self.input_file)
            return md_content
        except Exception as e:
            print(f"pymupdf4llm conversion error: {str(e)}")
            return f"Conversion with pymupdf4llm failed: {str(e)}"

    def docling_conversion(self):
        """Convert PDF to markdown using docling"""
        try:
            doc = DocumentConverter()
            conversion_result = doc.convert(self.input_file)
            md_content = conversion_result.document.export_to_markdown()
            return md_content

        except Exception as e:
            print(f"docling conversion error: {str(e)}")
            return f"Conversion with docling failed: {str(e)}"


    def pdfplumber_conversion(self):
        """Extracts text with headings and tables from a PDF while maintaining structure."""
        structured_text = []
        
        with pdfplumber.open(self.input_file) as pdf:
            for page_num, page in enumerate(pdf.pages, start=1):
                text_blocks = page.extract_words()  # Extract text blocks
                char_data = page.objects.get("char", [])  # Get character-level metadata

                font_sizes = [char["size"] for char in char_data if "size" in char]  # Extract font sizes
                avg_font_size = sum(font_sizes) / len(font_sizes) if font_sizes else 12  # Default to 12 if unknown

                last_font_size = avg_font_size  # Base reference font size

                # Process each word and infer headings based on font size
                for word in text_blocks:
                    text = word["text"]
                    
                    # Find corresponding font size (fallback to avg)
                    word_font_size = next((char["size"] for char in char_data if char["text"] == text), avg_font_size)

                    # Heading detection: If font size is significantly larger than the average, assume heading
                    if word_font_size > avg_font_size * 1.2:  # 20% larger than avg
                        structured_text.append(f"\n# {text}\n")  # Markdown heading
                    else:
                        structured_text.append(text)

                # Extract Tables
                tables = page.extract_tables()
                for table in tables:
                    structured_text.append("\n| " + " | ".join(table[0]) + " |\n")  # Markdown Table Header
                    structured_text.append("|" + " --- |" * len(table[0]))  # Table divider
                    for row in table[1:]:
                        structured_text.append("| " + " | ".join(row) + " |")

                structured_text.append("\n---\n")  # Page separator
        return "\n".join(structured_text)
    

    def openai_vision_conversion(self):
        client = openai.OpenAI()
        # Convert PDF pages to images
        images = convert_from_path(self.input_file, dpi=300)
        markdown_text = ""

        for i, image in enumerate(images):
            # Convert PIL image to bytes
            img_byte_arr = io.BytesIO()
            image.save(img_byte_arr, format='PNG')
            img_byte_arr = img_byte_arr.getvalue()

            # Encode image in base64
            base64_image = base64.b64encode(img_byte_arr).decode("utf-8")

            prompt = "Extract the text content from the provided image while preserving structural elements such as titles, headings, lists, code blocks, and tables. Format the output in valid Markdown without enclosing the entire content within triple backticks. Maintain the original hierarchy of headings and ensure correct Markdown syntax for all elements."
            # Call OpenAI's GPT-4 with vision capabilities
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            { "type": "text", "text": f"{prompt}" },
                            {
                                "type": "image_url",
                                "image_url": {
                                    "url": f"data:image/jpeg;base64,{base64_image}",
                                },
                            },
                        ],
                    }
                ],
            )
            # Extract text from the response
            page_text = response.choices[0].message.content

            # Append to the markdown text with a page separator
            markdown_text += f"\n\n## Page {i + 1}\n\n{page_text}\n\n"

        return markdown_text


    def openai_conversion(self, text):
        """Sends structured text to OpenAI to enhance Markdown formatting."""
        client = openai.OpenAI()
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "system", "content": (
                "You are a Markdown conversion assistant. Your task is to convert the following extracted text into properly formatted Markdown, "
                "preserving ALL content exactly as given. DO NOT summarize, shorten, or omit any details. "
                "Ensure that:\n"
                "- All headings are formatted correctly using `#`, `##`, `###`.\n"
                "- Tables are formatted using Markdown syntax (`| Column1 | Column2 |` with proper dividers).\n"
                "- Lists are converted to `-` for bullet points or `1.` for numbered lists.\n"
                "- Any section breaks, page numbers, and special characters are kept as-is.\n"
                "- Inline formatting (bold, italic) is preserved if present.\n"
                "- Code blocks are wrapped in triple backticks if detected.\n"
                "The goal is to reproduce the document in Markdown EXACTLY as it appears in the extracted text.\n"
                "Format the output in valid Markdown without enclosing the entire content within triple backticks. Maintain the original hierarchy of headings and ensure correct Markdown syntax for all elements."
            )},
                    {"role": "user", "content": text}]
        )
        markdown_content = response.choices[0].message.content
        return markdown_content

    def mistralai_conversion(self):
        def transform_image_references(text):
            pattern = r"!\[(.*?)\]\((img-\d+)\.(jpeg|jpg|png|gif)\)"
            
            def replace_match(match):
                filename = match.group(2)  # Extract "img-12"
                extension = match.group(3)  # Extract "jpeg"
                new_filename = filename.replace("-", "_") + ".png"
                return f"![{new_filename}]({IMAGE_FOLDER}/{new_filename})"
            
            return re.sub(pattern, replace_match, text)
        
        api_key = os.environ["MISTRAL_API_KEY"]
        client = Mistral(api_key=api_key)
        uploaded_pdf = client.files.upload(
            file={
                "file_name": self.input_file.name,
                "content": open(self.input_file, "rb"),
            },
            purpose="ocr"
        )  
        signed_url = client.files.get_signed_url(file_id=uploaded_pdf.id)
        ocr_response = client.ocr.process(
            model="mistral-ocr-latest",
            document={
                "type": "document_url",
                "document_url": signed_url.url,
            }
        )
        pages = [page.markdown for page in ocr_response.pages]
        markdown_text = '\n\n'.join(pages)
        markdown_text = transform_image_references(markdown_text)
        return markdown_text

    def zip_markdown_doc_with_images(self):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip_file:
            temp_zip_path = Path(tmp_zip_file.name)  # Get the temp file path

        with zipfile.ZipFile(temp_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            # Add the output file to the root of the zip archive
            if self.output_file.exists():
                zipf.write(self.output_file, self.output_file.name)

            # Add image files inside "images/" directory in the zip archive
            if self.doc_image_folder.exists():
                for root, _, files in os.walk(self.doc_image_folder):
                    for file in files:
                        file_path = Path(root) / file
                        zipf.write(file_path, Path("images") / file)
        return Path(temp_zip_path)

    def get_zipped_images(self):
        shutil.make_archive(self.doc_image_folder, 'zip', self.doc_image_folder)
        return f"{self.doc_image_folder}.zip"
    
    def get_file_download_link(self, link_text: str):
        """Generate a download link for an existing file"""
        if self.create_image_zip_file and self.output_file.exists():
            zip_file = self.zip_markdown_doc_with_images()
            with zip_file.open("rb") as f:
                bytes_data = f.read()
            b64 = base64.b64encode(bytes_data).decode()
            mime_type = "application/zip"
            href = f'<a href="data:file/{mime_type};base64,{b64}" download="{zip_file.name}">{link_text}</a>'
            return href
        elif self.output_file.exists():
            with self.output_file.open("rb") as f:
                bytes_data = f.read()
            b64 = base64.b64encode(bytes_data).decode()
            mime_type = "application/pdf" if self.output_file.suffix == ".pdf" else "text/markdown"
            filename = os.path.basename(self.output_file)
            href = f'<a href="data:file/{mime_type};base64,{b64}" download="{filename}">{link_text}</a>'
            return href
    
    def convert(self):
        md_content = ""
        if self.lib.lower() == 'docling':
            self.md_content = self.docling_conversion()
        elif self.lib.lower() == 'pymupdf4llm':
            self.md_content = self.pymupdf4llm_conversion()
        elif self.lib.lower() == 'pdfplumber+chatgpt-4o':
            self.md_content = self.pdfplumber_conversion()
            self.md_content = self.openai_conversion(md_content)
        elif self.lib.lower() == 'chatgpt-4o-vision':
            self.md_content = self.openai_vision_conversion()
        elif self.lib.lower() == 'mistral-ocr':
            self.md_content = self.mistralai_conversion()
            self.extract_images_from_pdf()
        else:
            self.md_content = self.pymupdf_conversion()
        with open(self.output_file, "w") as f:
            f.write(self.md_content)