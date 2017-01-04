use obs;
DROP TABLE PN_TICKETS_ENRICHIS;
CREATE EXTERNAL TABLE PN_TICKETS_ENRICHIS(
num_ticket STRING,
pht_idtcom STRING,
nombre_der_clos_produit_3_mois STRING,
is_repetitif STRING,
identifiant_1_produit STRING,
ce_id STRING, 
connexion_id STRING,
support_id STRING,
identifiant_sous_reseau STRING, 
service_support STRING,
collect STRING,
support STRING, 
nb_paires STRING,
collect_role STRING,
type_produit STRING, 
router_role STRING,
datemescom BIGINT,
etat_produit STRING,
identifiant_2_produit STRING,
identifiant_3_produit STRING,
identifiant_4_produit STRING, 
identifiant_eds_pilote STRING,
poste_utilisateur STRING,
gtr STRING, 
plage_horaire_gtr STRING,
gtr_respecte STRING,
origine STRING, 
initiateur_nom_utilisateur STRING,
date_debut_ticket BIGINT,
date_creation_ticket BIGINT,
date_retablissement BIGINT,
date_cloture_ticket BIGINT,
duree_indisponibilite INT,
duree_indisponibilite_hors_gel INT,
duree_constractuelle_indispo STRING,
year_date INT,
mois_cloture_ticket STRING,
mois_cloture_ticket_indus STRING,
sem_cloture_ticket STRING,
raison_sociale_ticket STRING,
raison_sociale STRING,
addresse_complete_client STRING, 
code_postal_client STRING,
siren STRING,
societe_extremite_a STRING, 
ville_extremite_a STRING,
voie_extremite_a STRING,
cp_extremite_a STRING,
societe_extremite_b STRING,
ville_extremite_b STRING,
voie_extremite_b STRING, 
cp_extremite_b STRING,
bas_id STRING, 
category STRING, 
description STRING,
nature_initiale STRING,
nature_finale STRING, 
responsabilite_pbm STRING,
famille_de_probleme STRING,
libelle_imputation STRING, 
subcategory STRING,
detail_probleme STRING,
complement_interne STRING, 
cause_nrgtr STRING,
responsabilite_nrgtr STRING,
donnee_complementaire STRING,
libelle_succinct STRING,
constructeur STRING,
chassis STRING, 
ios_version STRING,
ios_version_from_iai STRING,
ip_admin STRING, 
dslam_id STRING,
master_dslam_id STRING,
nortel_id STRING, 
pe_id STRING,
fav_id STRING,
ntu_id STRING, 
tronc_type STRING,
is_gtrisable STRING,
population STRING, 
population_cat STRING,
rap STRING, 
type_ticket STRING,
dependance_ticket STRING,
imputation_princ STRING,
eds_active_1 STRING, 
eds_active_2 STRING,
eds_active_3 STRING,
eds_active_4 STRING, 
eds_active_5 STRING,
poste_active STRING,
dt_deb_suivi BIGINT, 
dt_fin_suivi BIGINT,
dt_deb_pec BIGINT,
delai_activation_pec STRING, 
duree_gel STRING,
duree_totale STRING,
tic_dg_auto INT, 
num_pivot STRING,
element_reseau STRING,
type_interne STRING)
COMMENT 'Creating PN_TICKETS_ENRICHIS table'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\073'
STORED AS TEXTFILE
LOCATION '${hivevar:ticket_location}';