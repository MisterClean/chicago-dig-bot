"""Test script for contractor name normalization."""
from analytics.stats import StatsGenerator
import sys

def test_normalization():
    """Test contractor name normalization with various cases."""
    
    # Initialize StatsGenerator
    stats = StatsGenerator()
    
    # Test cases with expected normalized output
    test_cases = {
        # Department of Water Management variations
        "DWM": "Department of Water Management",
        "CITY OF CHICAGO DEPT OF WATER": "Department of Water Management",
        "CHICAGO DEPT WATER MANAGEMENT": "Department of Water Management",
        "DEPT OF WATER MANAGEMENT": "Department of Water Management",
        
        # Construction abbreviations
        "SUMIT NSTRUCTION": "Sumit Construction",
        "SUMIT CONST": "Sumit Construction",
        "CABO NSTRUCTION": "Cabo Construction",
        "CABO CONST": "Cabo Construction",
        
        # Peoples Gas variations
        "PEOPLES GAS": "Peoples Gas",
        "PEOPLE GAS": "Peoples Gas",
        "PEOPLES GAS LIGHT & COKE": "Peoples Gas",
        "INTEGRYS ENERGY GROUP / PEOPLES GAS": "Peoples Gas",
        
        # Business suffix variations
        "ACME CONSTRUCTION INC.": "Acme Construction Inc",
        "ACME CONSTRUCTION INCORPORATED": "Acme Construction Inc",
        "ACME CONST. CO.": "Acme Construction Co",
        "ACME CONSTRUCTION COMPANY": "Acme Construction Co",
        
        # Special characters and formatting
        "ABC PLBG & HTG": "ABC Plumbing & Heating",
        "XYZ EXCAV. & CONST.": "XYZ Excavating & Construction",
        "SMITH CONSTR. (SL-1234)": "Smith Construction (SL-1234)",
        "JONES PLBG (SEAL)": "Jones Plumbing (SEAL)",
        
        # Case variations
        "chicago concrete": "Chicago Concrete",
        "CHICAGO CONCRETE": "Chicago Concrete",
        "Chicago Concrete": "Chicago Concrete",
        
        # Ampersand variations
        "A AND B CONSTRUCTION": "A & B Construction",
        "A&B CONSTRUCTION": "A & B Construction",
        "A & B CONST": "A & B Construction",
        
        # Real examples from analysis
        "SEVEN-D CONSTRUCTION CO*": "Seven-D Construction Co",
        "M & J ASPHALT *": "M&J Asphalt",
        "PLUMBING PROFESSIONALS*": "Plumbing Professionals",
        "CABO CONSTRUCTION CORP*": "Cabo Construction Corp",
    }
    
    # Test each case
    passed = 0
    failed = 0
    
    print("\nTesting contractor name normalization:")
    print("=====================================")
    
    for input_name, expected in test_cases.items():
        result = stats._normalize_name(input_name)
        if result == expected:
            passed += 1
            print(f"✓ {input_name} -> {result}")
        else:
            failed += 1
            print(f"✗ {input_name} -> {result} (expected: {expected})")
    
    # Print summary
    total = passed + failed
    print("\nResults:")
    print(f"Passed: {passed}/{total} ({passed/total*100:.1f}%)")
    print(f"Failed: {failed}/{total} ({failed/total*100:.1f}%)")
    
    return failed == 0

if __name__ == "__main__":
    success = test_normalization()
    sys.exit(0 if success else 1)
