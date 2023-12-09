date=$(date '+%Y%m%d')
catmandu convert OAI --url http://oai.bn-r.fr/oai.php --listSets 1 --fix "copy_field(setDescription.0.description.0, set_description);retain(setSpec, setName, set_description)" to XLSX > results/oai_sets_bnr_description_$date.xlsx
