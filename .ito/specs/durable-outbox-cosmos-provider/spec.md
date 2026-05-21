<!-- ITO:START -->
## ADDED Requirements

### Requirement: Cosmos Provider Client
Cosmos stores SHALL use typed provider client operations for create/read/query/conditional update.

#### Scenario: concurrent claim
- **WHEN** two claim attempts target the same item
- **THEN** only one conditional update succeeds
<!-- ITO:END -->
