Based on the output you provided, I can see exactly what's happened. The API structure has indeed changed, and fields that used to have direct company address components are now represented differently.

Here's the mapping I see between your expected columns and what's actually in the dataframe:

### For Building Contractor:
- `buildingContractor_company_address_street` → `buildingContractor_legalEntity_persons_person_addressSwitzerland_street`
- `buildingContractor_company_address_houseNumber` → `buildingContractor_legalEntity_persons_person_addressSwitzerland_houseNumber`
- `buildingContractor_company_address_swissZipCode` → `buildingContractor_legalEntity_persons_person_addressSwitzerland_swissZipCode`
- `buildingContractor_company_address_town` → `buildingContractor_legalEntity_persons_person_addressSwitzerland_town`

### For Project Framer:
- `projectFramer_company_address_street` → `projectFramer_persons_person_addressSwitzerland_street`
- `projectFramer_company_address_houseNumber` → `projectFramer_persons_person_addressSwitzerland_houseNumber`
- `projectFramer_company_address_swissZipCode` → `projectFramer_persons_person_addressSwitzerland_swissZipCode`
- `projectFramer_company_address_town` → `projectFramer_persons_person_addressSwitzerland_town`

### UIDs are completely missing:
- `buildingContractor_company_uid` - no direct equivalent
- `projectFramer_company_uid` - no direct equivalent

The key pattern change is that the structure has shifted from `company_address_X` to `legalEntity_persons_person_addressSwitzerland_X` or simply `persons_person_addressSwitzerland_X`. This suggests that the API now represents addresses through person entities rather than directly on the company.

You have two options:

1. **Modify your column renaming at the beginning** to transform these new column names into your expected ones:
```python
# Add these additional replacements to your existing ones
df.columns = df.columns.str.replace("_legalEntity_persons_person_addressSwitzerland_", "_company_address_")
df.columns = df.columns.str.replace("_persons_person_addressSwitzerland_", "_company_address_")
```

2. **Update your columns_of_interest** to use the new column names instead of the old ones.

The first option is cleaner since it keeps your downstream processing unchanged and just adapts to the API's new structure.

For the missing UID fields, you might need to handle them separately by setting them to empty values or finding alternative identifiers in the data.