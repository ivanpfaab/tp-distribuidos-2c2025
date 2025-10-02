# Dynamic FileID Assignment Examples

## Current Files (as they would be assigned):

```
data/stores/stores.csv                           → ST01
data/transactions/transactions_202307.csv        → TR01
data/transactions/transactions_202308.csv        → TR02
data/transaction_items/transaction_items_202307.csv → TI01
data/transaction_items/transaction_items_202308.csv → TI02
data/users/users_202307.csv                     → US01
data/users/users_202308.csv                      → US02
data/menu_items/menu_items.csv                  → MN01
data/payment_methods/payment_methods.csv          → PM01
data/vouchers/vouchers.csv                       → VC01
```

## Future File Scenarios:

### Adding more transaction files:
```
data/transactions/transactions_202309.csv        → TR03
data/transactions/transactions_202310.csv        → TR04
data/transactions/transactions_202311.csv        → TR05
```

### Adding more user files:
```
data/users/users_202309.csv                     → US03
data/users/users_202310.csv                     → US04
```

### Adding new file types:
```
data/products/products.csv                      → XX01 (unknown type)
data/inventory/inventory_202307.csv             → XX02 (unknown type)
```

### Adding more stores (if multiple store files exist):
```
data/stores/stores_backup.csv                   → ST02
data/stores/stores_updated.csv                  → ST03
```

## Benefits:

1. **Automatic numbering**: No need to manually assign numbers
2. **Scalable**: Supports up to 99 files per type (01-99)
3. **Deterministic**: Same files always get same FileID
4. **Extensible**: Easy to add new file types
5. **Future-proof**: Handles any number of files within limits
