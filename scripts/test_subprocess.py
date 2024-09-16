import subprocess

def run_script(dataset_name):
    """
    Runs the external script with the provided dataset name.
    """
    # Define the base command
    command = [
        'python3', 'scripts/image_scorers/clip_batch_calculations.py',
        '--minio-access-key', 'v048BpXpWrsVIHUfdAix',
        '--minio-secret-key', '4TFS20qkxVuX2HaC8ezAgG7GaDlVI1TqSPs0BKyu',
        '--bucket', 'external',
        '--dataset', dataset_name  # Dataset name without extra quotes
    ]
    
    try:
        # Print the command for debugging purposes
        print(f"Running command: {' '.join(command)}")
        
        # Run the command
        result = subprocess.run(command, check=True, text=True, capture_output=True)
        
        # Print output and error
        print(f"Output for dataset '{dataset_name}':", result.stdout)
        print(f"Error for dataset '{dataset_name}':", result.stderr)
    
    except subprocess.CalledProcessError as e:
        print(f"An error occurred for dataset '{dataset_name}': {e}")

# List of dataset names
datasets = [
    'cavalry-girls', 'keplerth', 'mark-of-the-ninja', 'towerfall',
    'the-rift-breaker', 'crypt-necrodancer', 'starcom-nexus', 'grim-nights',
    'fabular-once-upon-a-spacetime', 'king-arthurs-gold', 'gibbon-beyond-the-trees',
    'mistward', 'broforce', 'axiom-verge', 'dogolrax', 'axiom-verge 2', 'halo',
    'carrion', 'eufloria-hd', 'trappist', 'the-last-federation', 'last-command',
    'oxygen-not-included', 'rusted-moss', 'fallout 2', 'craft-the-world',
    'the-vagrant', 'rebel-transmute', 'metro-2033', 'dead-cells', 'until-we-die',
    'webbed', 'turbo-kid', 'chasm', 'metal-slug', 'terraria', 'fallout',
    'rogue-legacy', 'metro-exodus', 'ai-war', 'phoenotopia-awakening',
    'starbound', 'wayward', 'super-metroid-redesign-dataset', 'hardcore-mecha'
]

# Run the script for each dataset
for dataset in datasets:
    run_script(dataset)
