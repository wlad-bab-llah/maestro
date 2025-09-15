@staticmethod
def list_process_and_delete_files(MINIO_CONFIG, system, **context):
    print("🚀 DÉBUT DE LA MÉTHODE list_process_and_delete_files")
    print("=" * 80)
    
    # 1. Configuration MinIO
    BUCKET_NAME = MINIO_CONFIG.get("bucket_name", "data")
    ENDPOINT_URL = MINIO_CONFIG.get("endpoint_url", "http://minio:9000")
    ACCESS_KEY = MINIO_CONFIG.get("access_key", "admin")
    SECRET_KEY = MINIO_CONFIG.get("secret_key", "password")
    PATH_TO_DCO = MINIO_CONFIG.get("path_dco", "/opt/airflow/apps")
    
    print("📋 CONFIGURATION MINIO:")
    print(f"   🪣 Bucket: {BUCKET_NAME}")
    print(f"   🌐 Endpoint: {ENDPOINT_URL}")
    print(f"   🔑 Access Key: {ACCESS_KEY[:4]}***")
    print(f"   🔒 Secret Key: ***{SECRET_KEY[-4:]}")
    print(f"   📁 Path DCO: {PATH_TO_DCO}")
    print(f"   🎯 System: {system}")
    print("=" * 80)
    
    # 2. Connexion S3
    print("🔌 TENTATIVE DE CONNEXION S3...")
    try:
        s3 = boto3.client(
            's3',
            endpoint_url=ENDPOINT_URL,
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY
        )
        print("✅ Connexion S3 établie avec succès")
    except Exception as e:
        print(f"❌ ERREUR CONNEXION S3: {e}")
        raise
    
    # 3. Lecture du fichier DCO
    print(f"📖 LECTURE DU FICHIER DCO: {PATH_TO_DCO}/dco.txt")
    try:
        with open(f"{PATH_TO_DCO}/dco.txt", "r") as f:
            dco_retrieved = f.read().strip()
        print(f"✅ DCO lu avec succès: {dco_retrieved}")
    except Exception as e:
        print(f"❌ ERREUR LECTURE DCO: {e}")
        raise
    
    # 4. Test de connexion au bucket
    print(f"🪣 TEST DE CONNEXION AU BUCKET: {BUCKET_NAME}")
    try:
        bucket_exists = s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket {BUCKET_NAME} accessible")
    except Exception as e:
        print(f"❌ ERREUR ACCÈS BUCKET {BUCKET_NAME}: {e}")
        # Essayer de lister les buckets disponibles
        try:
            buckets = s3.list_buckets()
            print("📋 Buckets disponibles:")
            for bucket in buckets.get('Buckets', []):
                print(f"   - {bucket['Name']}")
        except Exception as list_error:
            print(f"❌ Impossible de lister les buckets: {list_error}")
        raise
    
    # 5. Listing des objets dans le bucket
    print(f"📁 LISTING DES OBJETS DANS LE BUCKET {BUCKET_NAME}...")
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME)
        total_objects = len(response.get('Contents', []))
        print(f"📊 Nombre total d'objets dans le bucket: {total_objects}")
        
        if total_objects == 0:
            print("⚠️ BUCKET VIDE - Aucun objet trouvé")
            raise AirflowSkipException("Bucket vide - aucun fichier à traiter")
        
        # Afficher tous les objets pour debug
        print("📋 TOUS LES OBJETS DANS LE BUCKET:")
        for i, obj in enumerate(response.get('Contents', []), 1):
            print(f"   {i:3d}. {obj['Key']} (taille: {obj['Size']} bytes, modifié: {obj['LastModified']})")
            
    except Exception as e:
        print(f"❌ ERREUR LISTING OBJETS: {e}")
        raise
    
    # 6. Filtrage par système
    print(f"🔍 FILTRAGE DES FICHIERS PAR SYSTÈME: '{system}'")
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].startswith(system)]
    print(f"📊 Nombre de fichiers correspondant au système '{system}': {len(files)}")
    
    if len(files) == 0:
        print(f"⚠️ AUCUN FICHIER TROUVÉ POUR LE SYSTÈME '{system}'")
        print("🔍 Vérifiez que:")
        print(f"   - Les fichiers commencent bien par '{system}'")
        print(f"   - Le nom du système est correct")
        print("   - Les fichiers sont bien uploadés dans le bucket")
    else:
        print(f"📋 FICHIERS TROUVÉS POUR LE SYSTÈME '{system}':")
        for i, file_key in enumerate(files, 1):
            print(f"   {i:3d}. {file_key}")
    
    # 7. Récupération des fichiers déjà traités
    print("🔄 RÉCUPÉRATION DES FICHIERS DÉJÀ TRAITÉS (XCom)...")
    ti = context['ti']
    processed_files = ti.xcom_pull(task_ids='list_and_process_new_files', key='processed_files') or []
    print(f"📊 Nombre de fichiers déjà traités: {len(processed_files)}")
    
    if processed_files:
        print("📋 FICHIERS DÉJÀ TRAITÉS:")
        for i, file_key in enumerate(processed_files, 1):
            print(f"   {i:3d}. {file_key}")
    else:
        print("✅ Aucun fichier déjà traité (première exécution)")
    
    # 8. Calcul des nouveaux fichiers
    print("🆕 CALCUL DES NOUVEAUX FICHIERS À TRAITER...")
    new_files = list(set(files) - set(processed_files))
    print(f"📊 Nombre de nouveaux fichiers: {len(new_files)}")
    
    if len(new_files) == 0:
        print("⚠️ AUCUN NOUVEAU FICHIER À TRAITER")
        print("🔍 Raisons possibles:")
        print("   - Tous les fichiers ont déjà été traités")
        print("   - Aucun fichier ne correspond au système spécifié")
        print("   - Les fichiers ne sont pas présents dans le bucket")
        raise AirflowSkipException("No files to process")
    else:
        print("📋 NOUVEAUX FICHIERS À TRAITER:")
        for i, file_key in enumerate(new_files, 1):
            print(f"   {i:3d}. {file_key}")
    
    # 9. Validation des dates
    print("📅 VALIDATION DES DATES DANS LES NOMS DE FICHIERS...")
    print(f"📅 Date DCO attendue: {dco_retrieved}")
    
    execution_date = dco_retrieved
    for file_key in new_files:
        print(f"🔍 Analyse du fichier: {file_key}")
        date_from_file = Functionnalities.extract_date_from_filename(system, file_key)
        
        if date_from_file:
            print(f"   ✅ Date extraite: {date_from_file}")
            if not execution_date == date_from_file:
                error_msg = f"⛔ ERREUR DE DATE - Attendue: {execution_date}, Trouvée: {date_from_file}"
                print(error_msg)
                raise AirflowSkipException(error_msg)
            else:
                print(f"   ✅ Date validée: {date_from_file}")
        else:
            print(f"   ⚠️ Impossible d'extraire la date du fichier: {file_key}")
    
    # 10. Traitement et suppression des fichiers
    print("🔄 TRAITEMENT ET SUPPRESSION DES FICHIERS...")
    for file_key in new_files:
        print(f"🎯 Traitement du fichier: {file_key}")
        # Your processing logic here
        
        # Delete file after processing
        try:
            s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)
            print(f"🗑️ Fichier supprimé avec succès: {file_key}")
        except Exception as e:
            print(f"❌ Échec de suppression {file_key}: {e}")
    
    # 11. Sauvegarde XCom
    print("💾 SAUVEGARDE DES DONNÉES XCOM...")
    ti.xcom_push(key='processed_files', value=processed_files + new_files)
    ti.xcom_push(key='execution_date', value=execution_date)
    
    print(f"📅 Execution date sauvegardée: {execution_date}")
    print(f"📊 Total fichiers traités sauvegardés: {len(processed_files + new_files)}")
    
    print("=" * 80)
    print("🎉 FIN DE LA MÉTHODE list_process_and_delete_files - SUCCÈS")
    print("=" * 80)
