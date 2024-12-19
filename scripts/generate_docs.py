import os
import sys
import argparse
from typing import List, Optional
import subprocess

class DocumentationGenerator:
    def __init__(self, source_dir: str, output_dir: str):
        self.source_dir = source_dir
        self.output_dir = output_dir
    
    def generate_api_docs(self):
        """Generate API documentation using sphinx-apidoc."""
        cmd = [
            'sphinx-apidoc',
            '-f',  # Force overwrite
            '-o', f'{self.output_dir}/api',  # Output directory
            self.source_dir,  # Source code directory
            '**/test_*.py',  # Exclude test files
            '**/setup.py',   # Exclude setup files
        ]
        subprocess.run(cmd, check=True)
    
    def build_docs(self):
        """Build documentation using sphinx-build."""
        cmd = [
            'sphinx-build',
            '-b', 'html',  # Build format
            '-d', f'{self.output_dir}/doctrees',  # Cache directory
            'docs/source',  # Source directory
            f'{self.output_dir}/html'  # Output directory
        ]
        subprocess.run(cmd, check=True)
    
    def generate_openapi_spec(self):
        """Generate OpenAPI specification."""
        from agnes.api.routes import app
        import json
        
        openapi_spec = app.openapi()
        spec_path = f'{self.output_dir}/openapi.json'
        
        with open(spec_path, 'w') as f:
            json.dump(openapi_spec, f, indent=2)
    
    def generate_all(self):
        """Generate all documentation."""
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Generate documentation
        self.generate_api_docs()
        self.generate_openapi_spec()
        self.build_docs()

def main():
    parser = argparse.ArgumentParser(description='Generate AGNES documentation')
    parser.add_argument('--source', default='src/agnes',
                      help='Source code directory')
    parser.add_argument('--output', default='docs/build',
                      help='Output directory')
    
    args = parser.parse_args()
    
    generator = DocumentationGenerator(args.source, args.output)
    generator.generate_all()

if __name__ == '__main__':
    main()
