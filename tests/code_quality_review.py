"""
Code quality review script for Winnie Da Pooh.
Reviews error handling, documentation, and code organization.
"""
import sys
from pathlib import Path
import ast
import re

sys.path.insert(0, str(Path(__file__).parent.parent))


def check_error_handling(filepath: Path) -> dict:
    """Check error handling in a Python file."""
    results = {
        "file": str(filepath),
        "try_except_count": 0,
        "bare_except_count": 0,
        "error_logged_count": 0,
        "issues": []
    }
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.ExceptHandler):
                results["try_except_count"] += 1
                
                # Check for bare except
                if node.type is None:
                    results["bare_except_count"] += 1
                    results["issues"].append(f"Line {node.lineno}: Bare except clause")
                
                # Check if error is logged (simple heuristic)
                for child in ast.walk(node):
                    if isinstance(child, ast.Call):
                        if hasattr(child.func, 'attr') and 'log' in child.func.attr.lower():
                            results["error_logged_count"] += 1
                            break
    
    except Exception as e:
        results["issues"].append(f"Could not parse file: {e}")
    
    return results


def check_docstrings(filepath: Path) -> dict:
    """Check for docstrings in functions and classes."""
    results = {
        "file": str(filepath),
        "total_functions": 0,
        "documented_functions": 0,
        "total_classes": 0,
        "documented_classes": 0,
        "issues": []
    }
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
        
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                results["total_functions"] += 1
                if ast.get_docstring(node):
                    results["documented_functions"] += 1
                else:
                    if not node.name.startswith('_'):  # Ignore private functions
                        results["issues"].append(f"Line {node.lineno}: Function {node.name} missing docstring")
            
            elif isinstance(node, ast.ClassDef):
                results["total_classes"] += 1
                if ast.get_docstring(node):
                    results["documented_classes"] += 1
                else:
                    results["issues"].append(f"Line {node.lineno}: Class {node.name} missing docstring")
    
    except Exception as e:
        results["issues"].append(f"Could not parse file: {e}")
    
    return results


def check_imports(filepath: Path) -> dict:
    """Check import organization."""
    results = {
        "file": str(filepath),
        "total_imports": 0,
        "issues": []
    }
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            tree = ast.parse(content)
        
        imports = []
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                results["total_imports"] += 1
                imports.append(node.lineno)
        
        # Check if imports are at the top (allowing for docstring)
        if imports:
            first_import_line = min(imports)
            if first_import_line > 20:  # Allow some space for module docstring
                results["issues"].append(f"First import at line {first_import_line} (imports should be near top)")
    
    except Exception as e:
        results["issues"].append(f"Could not parse file: {e}")
    
    return results


def review_file(filepath: Path):
    """Comprehensive review of a single file."""
    print(f"\n{'='*80}")
    print(f"Reviewing: {filepath.name}")
    print(f"{'='*80}")
    
    error_results = check_error_handling(filepath)
    doc_results = check_docstrings(filepath)
    import_results = check_imports(filepath)
    
    # Error Handling
    print(f"\nError Handling:")
    print(f"  Try/except blocks: {error_results['try_except_count']}")
    print(f"  Bare except clauses: {error_results['bare_except_count']}")
    print(f"  Errors logged: {error_results['error_logged_count']}")
    if error_results['issues']:
        print(f"  Issues:")
        for issue in error_results['issues'][:5]:
            print(f"    - {issue}")
    
    # Documentation
    print(f"\nDocumentation:")
    print(f"  Functions: {doc_results['documented_functions']}/{doc_results['total_functions']} documented")
    print(f"  Classes: {doc_results['documented_classes']}/{doc_results['total_classes']} documented")
    if doc_results['issues']:
        print(f"  Missing docstrings: {len(doc_results['issues'])}")
        for issue in doc_results['issues'][:3]:
            print(f"    - {issue}")
    
    # Imports
    print(f"\nImports:")
    print(f"  Total imports: {import_results['total_imports']}")
    if import_results['issues']:
        for issue in import_results['issues']:
            print(f"    - {issue}")
    
    return {
        "error_handling": error_results,
        "documentation": doc_results,
        "imports": import_results
    }


def main():
    """Run code quality review on key files."""
    files_to_review = [
        "src/build_unified_parquet.py",
        "runner/runner.py",
        "runner/evaluator.py",
        "methods/base.py",
        "dataobject/tasks/base.py",
        "dataobject/dataset.py"
    ]
    
    results = {}
    for filepath_str in files_to_review:
        filepath = Path(filepath_str)
        if filepath.exists():
            results[filepath_str] = review_file(filepath)
        else:
            print(f"\nWarning: {filepath} not found")
    
    # Summary
    print(f"\n{'='*80}")
    print(f"SUMMARY")
    print(f"{'='*80}")
    
    total_try_except = sum(r["error_handling"]["try_except_count"] for r in results.values())
    total_bare_except = sum(r["error_handling"]["bare_except_count"] for r in results.values())
    total_functions = sum(r["documentation"]["total_functions"] for r in results.values())
    documented_functions = sum(r["documentation"]["documented_functions"] for r in results.values())
    
    print(f"\nError Handling:")
    print(f"  Total try/except blocks: {total_try_except}")
    print(f"  Bare except clauses: {total_bare_except}")
    
    print(f"\nDocumentation:")
    print(f"  Function documentation rate: {documented_functions}/{total_functions} ({100*documented_functions/total_functions:.1f}%)")
    
    if total_bare_except > 0:
        print(f"\nWARNING: Found {total_bare_except} bare except clauses (should specify exception type)")
    
    if documented_functions / total_functions < 0.5:
        print(f"\nWARNING: Low documentation rate ({100*documented_functions/total_functions:.1f}%)")


if __name__ == "__main__":
    main()
