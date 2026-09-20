| Injected error | Rows injected | Detected | Detection rate |
|---|---|---|---|
| Price multiplied by 100 | 500 | 440 | 88.0 % |
| Price made negative | 500 | 500 | 100.0 % |
| Extra digit typed in beds (2 becomes 23) | 500 | 258 | 51.6 % |
| Review rating set to 7.5 | 500 | 500 | 100.0 % |
| first_review and last_review swapped | 500 | 500 | 100.0 % |
| Latitude shifted by 5 degrees (about 550 km) | 500 | 500 | 100.0 % |
| Latitude shifted by 0.05 degrees (about 5 km) | 500 | 0 | 0.0 % |
| Free text replaced by a phone number | 500 | 500 | 100.0 % |
| Row duplicated (same id) | 500 | 500 | 100.0 % |

Overall: 3698 of 4500 injected errors detected (82.2 %).
Rows quarantined that were neither injected nor already quarantined before the experiment: 0.
Seed: 42. 500 errors per type, on distinct rows that were valid in the original data.
