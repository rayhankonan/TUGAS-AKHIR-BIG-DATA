"""Execute all notebooks using this interpreter; stop on any cell error."""
from pathlib import Path
import sys, os, json
ROOT=Path(__file__).resolve().parents[1]
import nbformat
from nbclient import NotebookClient
kernel_root=ROOT/'.runtime/jupyter'
kernel_dir=kernel_root/'kernels/big_data'
kernel_dir.mkdir(parents=True,exist_ok=True)
(kernel_dir/'kernel.json').write_text(json.dumps({'argv':[sys.executable,'-m','ipykernel_launcher','-f','{connection_file}'],'display_name':'Big Data Python','language':'python'}),encoding='utf-8')
os.environ['JUPYTER_PATH']=str(kernel_root)+os.pathsep+os.environ.get('JUPYTER_PATH','')
paths = [ROOT/'notebooks'/sys.argv[1]] if len(sys.argv)>1 else sorted((ROOT/'notebooks').glob('*.ipynb'))
for path in paths:
    nb=nbformat.read(path,as_version=4)
    print('Executing',path.name,flush=True)
    client=NotebookClient(nb,timeout=900,kernel_name='big_data',resources={'metadata':{'path':str(ROOT)}})
    client.execute()
    nbformat.write(nb,path)
    print('Completed',path.name,flush=True)
