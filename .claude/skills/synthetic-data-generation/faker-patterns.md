# Faker Patterns

## Overview

Common patterns for using Faker to generate realistic synthetic data.

---

## TODO: Add Your Faker Patterns

This file is a placeholder for you to document common Faker patterns relevant to your use cases.

### Suggested Sections

**Basic Data Types**:

- Names, emails, addresses
- Phone numbers, dates, timestamps
- Text content, URLs
- Numbers, currencies

**Business-Specific Patterns**:

- Your domain-specific data patterns
- Custom providers
- Data relationships

**Example Structure**:

```python
from faker import Faker
fake = Faker()

# Generate customer data
customer = {
    'name': fake.name(),
    'email': fake.email(),
    'phone': fake.phone_number(),
    'address': fake.address()
}
```

---

## Useful Faker Providers

See [Faker Documentation](https://faker.readthedocs.io/en/master/providers.html) for complete list:

- Standard Providers (names, addresses, dates, etc.)
- Community Providers (specific domains)
- Custom Providers (define your own)

---

## Tips for Realistic Data

1. **Seed for reproducibility**: `Faker.seed(42)`
2. **Localization**: `Faker('en_US')` or `Faker('en_GB')`
3. **Consistent relationships**: Use same seed for related data
4. **Data quality issues**: Occasionally add nulls or edge cases
