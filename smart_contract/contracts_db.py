from shared_blockchain_structures import calculate_contract_id


class ContractAlreadyDeployed(Exception):
    pass


class SmartContractDatabase:
    """
        Contracts are derived from deploy transactions on the chain, so they are persisted
        with the chain and rebuilt (rebuild_from_chain) whenever the chain is loaded from
        disk or replaced by chain synchronisation. Deployed code is immutable.
    """
    def __init__(self):
        self.contracts = {}

    def store_contract(self, contract_id, code):
        existing = self.contracts.get(contract_id)
        if existing is not None and existing != code:
            raise ContractAlreadyDeployed(f"Contract '{contract_id}' is already deployed")
        self.contracts[contract_id] = code

    def get_contract(self, contract_id):
        return self.contracts.get(contract_id)

    def rebuild_from_chain(self, blocks):
        self.contracts = {}
        for block in blocks:
            if not getattr(block, "is_valid", True):
                continue
            for tx in block.transactions:
                if tx.receiver == "deploy":
                    self.contracts.setdefault(calculate_contract_id(tx.sender, tx.ts), tx.payload[0])
        return self.contracts
