# glaciar-vault-cleanup

Cleanup Glacier archives CLI

1. List Vaults
2. List Archives with job initiation
3. Clean up glacier archives

## How to use
```bash
$ cleanyp.py -h 

usage: cleanup.py [-h] [-listvault] [-listarchives] [-vaultname VAULTNAME]
                  [-deleteall] -region REGION [-profile PROFILE]

Glacier Vault cleanup CLI
  -region REGION        List All Glacier

optional arguments:
  -h, --help            show this help message and exit
  -listvault            List All Glacier Vaults
  -vaultname VAULTNAME  Specify Vault Name 
  -listarchives         List All Glacier Vault Archives [ Vault Name is needed ]
  -deleteall            Delete All archives [ Vaultname is needed ]
  -profile PROFILE      AWS Profile for credentials

```
## Examples
```bash


```