## Profiles
Each time duration in the list generates a **TSDB Block** for the duration. **realisticK8s** and **continuous** are factory methods that `produce functions` of type **PlanFn**, which then gets called with additional input parameters

### What we did / things of note
- **tenant_id** is a label that describes which cluster generated that data. This can be manually modified in the **meta.json** file to mock up data.
- Blocks can be uploaded to the **AWS S3** bucket manually
- The **tenant_id** does not matter when displaying the data in Grafana. The system does not filter by the **tenant_id** like we thought, so all blocks will show up in Grafana regardless of the anotation
- When uploading blocks into the **S3** bucket, the block folder name does not matter. Most likely, the **ulid** and **compaction sources** do though.
- When **compaction** occurs, the **source** is updated from whatever it was before (in the case of thanosbench, the source has been **blockgen**) to **compact**.


### What we want to investigate
- what if we have a "hub of hubs" (HoH) where each of the "child hubs" will send *non-compressed raw data* up to the **S3** bucket of the HoH. That way, the compaction can happen differently for each level. The child hubs can compact every 10 days, and the HoH can compact every 2 for example.
- Another thing we could investigate is what happens if each of the hub clusters use *the same S3 bucket* and we have a singleton compactor
- Add more than one metric