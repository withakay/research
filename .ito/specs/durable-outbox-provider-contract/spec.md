## MODIFIED Requirements

### Requirement: Single-Winner Claims
Provider-backed stores SHALL use provider concurrency tokens so only one dispatcher can own a claim transition.

#### Scenario: shared provider client has concurrent claimers
- **WHEN** two stores claim from the same provider state
- **THEN** one owner wins and losing races do not return duplicate claims
