# Migrate from 1.X.Y to 1.4.X

This upgrade comes with a new dedicated system schema which is used to store information about ledgers.
Also, schema of ledgers has been optimized for writes, so we have to alter it.

The system schema will automatically be created on any ledger access, but we can initialize it manually.

After an upgrade, if you list ledgers, you will see empty result :
```
$ numary storage list
No ledger found.
```

To populate the system schema, you can launch the following command :
```
$ numary storage scan
Registering ledger 'test'
Ledger 'test' registered
```

It this example, the system discovered one ledger called 'test'.

Now, if we list ledgers, we got another result :
```
$ numary storage list
Ledgers:
- test
```

After that, to update a specific ledger schema, we can use the following command :
```
$ numary storage upgrade
Storage 'test' migrated
```