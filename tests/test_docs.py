import pytest
import os
from scripts.generate_docs import DocumentationGenerator

@pytest.fixture
def doc_generator():
    return DocumentationGenerator(
        source_dir='src/agnes',
        output_dir='tests/test_docs'
    )

def test_api_docs_generation(doc_generator):
    doc_generator.generate_api_docs()
    assert os.path.exists('tests/test_docs/api/agnes.rst')

def test_openapi_spec_generation(doc_generator):
    doc_generator.generate_openapi_spec()
    assert os.path.exists('tests/test_docs/openapi.json')

def test_full_documentation_build(doc_generator):
    doc_generator.generate_all()
    assert os.path.exists('tests/test_docs/html/index.html')
