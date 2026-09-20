| Regle | Avant nettoyage | Apres nettoyage |
|---|---|---|
| id non nul | PASS | PASS |
| id unique | PASS | PASS |
| price > 0 et <= 10 000 | FAIL (31 lignes, 0.064 %) | PASS |
| price renseigne dans au moins 95 % des lignes | FAIL (29277 lignes, 37.69 %) | FAIL (29269 lignes, 37.703 %) |
| beds <= 20 | FAIL (4 lignes, 0.009 %) | PASS |
| bedrooms <= 15 | FAIL (11 lignes, 0.017 %) | PASS |
| bathrooms <= 10 | FAIL (5 lignes, 0.011 %) | PASS |
| review_scores_accuracy dans ]0, 5] | FAIL (9 lignes, 0.015 %) | PASS |
| review_scores_checkin dans ]0, 5] | FAIL (9 lignes, 0.015 %) | PASS |
| review_scores_cleanliness dans ]0, 5] | FAIL (9 lignes, 0.015 %) | PASS |
| review_scores_communication dans ]0, 5] | FAIL (7 lignes, 0.011 %) | PASS |
| review_scores_location dans ]0, 5] | FAIL (8 lignes, 0.013 %) | PASS |
| review_scores_rating dans ]0, 5] | FAIL (3 lignes, 0.005 %) | PASS |
| review_scores_value dans ]0, 5] | FAIL (8 lignes, 0.013 %) | PASS |
| last_review >= first_review | PASS | PASS |
| latitude dans la boite de Paris | PASS | PASS |
| longitude dans la boite de Paris | PASS | PASS |
| host_about pas uniquement numerique | FAIL (184 lignes, 0.514 %) | PASS |

Regles respectees : 5/18 avant, 17/18 apres.
Lignes : 77679 avant, 77631 apres.
Colonnes : 90 avant, 80 apres (colonnes entierement vides : 12 -> 0).
