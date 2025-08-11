import re

class StandardCheckExtractor:
    def __init__(self):
        """Initialize the Standard Check extractor."""
        # The specific paragraph to look for in standard paperwork
        self.STANDARD_PARAGRAPH = (
            "The fees set out in this Order Form Supplement are subject to change upon 90 days' written "
            "notice to Customer prior to the end of the then-current term and will become effective as of the "
            "first day of the following term if the Agreement (defined below) is renewed; provided, however, "
            "that Trulioo reserves the right to make changes to fees where necessary to comply with similar "
            "changes made by third party data sources, in which case Trulioo will, where possible, provide "
            "Customer with at least 30 days' prior notice of any such changes."
        )
        
        # Create variations that handle markdown formatting and page breaks
        self.STANDARD_PARAGRAPH_VARIATIONS = [
            # Original paragraph
            self.STANDARD_PARAGRAPH,
            
            # Lowercase version
            self.STANDARD_PARAGRAPH.lower(),
            
            # Uppercase version
            self.STANDARD_PARAGRAPH.upper(),
            
            # Normalized version (handles extra spaces, newlines, tabs)
            re.sub(r'\s+', ' ', self.STANDARD_PARAGRAPH).strip(),
            
            # Version that handles potential page breaks (newlines become spaces)
            re.sub(r'\n+', ' ', self.STANDARD_PARAGRAPH).strip(),
            
            # Version that removes markdown formatting characters
            re.sub(r'[*_`#]', '', self.STANDARD_PARAGRAPH).strip(),
            
            # Version that handles both markdown and whitespace normalization
            re.sub(r'[*_`#]', '', re.sub(r'\s+', ' ', self.STANDARD_PARAGRAPH)).strip(),
            
            # Version that removes punctuation but keeps words (for very loose matching)
            re.sub(r'[^\w\s]', ' ', self.STANDARD_PARAGRAPH).strip(),
        ]

    def check_standard_paragraph(self, markdown_text):
        """Check if the standard paragraph is present in the markdown text."""
        if not markdown_text:
            return False
        
        # Create multiple normalized versions of the markdown text to handle different formatting scenarios
        normalized_versions = [
            # Basic normalization (handles extra spaces, newlines, tabs)
            re.sub(r'\s+', ' ', markdown_text).strip(),
            
            # Handle page breaks by converting newlines to spaces
            re.sub(r'\n+', ' ', markdown_text).strip(),
            
            # Remove markdown formatting characters
            re.sub(r'[*_`#]', '', markdown_text).strip(),
            
            # Combine markdown removal with whitespace normalization
            re.sub(r'[*_`#]', '', re.sub(r'\s+', ' ', markdown_text)).strip(),
            
            # Remove punctuation but keep words (for very loose matching)
            re.sub(r'[^\w\s]', ' ', markdown_text).strip(),
        ]
        
        # Check for exact matches and variations against all normalized versions
        for normalized_text in normalized_versions:
            for variation in self.STANDARD_PARAGRAPH_VARIATIONS:
                if variation in normalized_text:
                    return True
        
        return False

    def extract_standard_check_data(self, json_data, markdown_text):
        """
        Check if the markdown contains the standard paragraph and add result to JSON.
        
        Args:
            json_data (dict): The JSON data to enrich
            markdown_text (str): The markdown text extracted from the PDF
            
        Returns:
            dict: Updated JSON data with StandardCheck section added
        """
        if not markdown_text:
            print("No markdown text provided.")
            return json_data
        
        print("Checking for standard paragraph in markdown...")
        
        # Check for standard paragraph
        has_standard_paragraph = self.check_standard_paragraph(markdown_text)
        
        print(f"Standard paragraph found: {'Yes' if has_standard_paragraph else 'No'}")
        
        # Add standard check result to the JSON
        if "StandardCheck" not in json_data:
            json_data["StandardCheck"] = {}
        json_data["StandardCheck"]["IsStandard"] = has_standard_paragraph
        
        print("Standard Check completed!")
        
        return json_data


# if __name__ == "__main__":
#     # Test the class
#     test_markdown = """
#     # Test Document
    
#     Some content here.
    
#     The fees set out in this Order Form Supplement are subject to change upon 90 days' written
#     notice to Customer prior to the end of the then-current term and will become effective as of the
#     first day of the following term if the Agreement (defined below) is renewed; provided, however,
#     that Trulioo reserves the right to make changes to fees where necessary to comply with similar
#     changes made by third party data sources, in which case Trulioo will, where possible, provide
#     Customer with at least 30 days' prior notice of any such changes.
    
#     More content here.
#     """
    
#     # Test with page break scenario
#     test_markdown_with_page_break = """
#     # Test Document
    
#     Some content here.
    
#     The fees set out in this Order Form Supplement are subject to change upon 90 days' written
    
#     notice to Customer prior to the end of the then-current term and will become effective as of the
#     first day of the following term if the Agreement (defined below) is renewed; provided, however,
#     that Trulioo reserves the right to make changes to fees where necessary to comply with similar
#     changes made by third party data sources, in which case Trulioo will, where possible, provide
#     Customer with at least 30 days' prior notice of any such changes.
    
#     More content here.
#     """
    
#     # Test with no standard paragraph
#     test_markdown_no_standard = """
#     # Test Document
    
#     Some content here.
    
#     This is just some random text that doesn't contain the standard paragraph.
    
#     More content here.
#     """
    
#     # Create extractor instance and process data
#     extractor = StandardCheckExtractor()
    
#     # Test JSON data
#     test_json = {"test": "data"}
    
#     print("=== Testing Normal Markdown ===")
#     result1 = extractor.extract_standard_check_data(test_json.copy(), test_markdown)
#     print(f"Result: {result1['StandardCheck']['IsStandard']}")
    
#     print("\n=== Testing Markdown with Page Break ===")
#     result2 = extractor.extract_standard_check_data(test_json.copy(), test_markdown_with_page_break)
#     print(f"Result: {result2['StandardCheck']['IsStandard']}")
    
#     print("\n=== Testing Markdown without Standard Paragraph ===")
#     result3 = extractor.extract_standard_check_data(test_json.copy(), test_markdown_no_standard)
#     print(f"Result: {result3['StandardCheck']['IsStandard']}")
    
#     print(f"\nSummary:")
#     print(f"Normal markdown: {'Standard' if result1['StandardCheck']['IsStandard'] else 'Non-standard'}")
#     print(f"Page break markdown: {'Standard' if result2['StandardCheck']['IsStandard'] else 'Non-standard'}")
#     print(f"No standard paragraph: {'Standard' if result3['StandardCheck']['IsStandard'] else 'Non-standard'}")
