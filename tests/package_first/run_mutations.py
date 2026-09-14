"""Run isolated, deliberately incorrect production edits against behavioral tests.

Every edit is restored in finally. Run only on a disposable validation checkout.
"""
from pathlib import Path
import json,subprocess,sys,shutil,os
ROOT=Path(sys.argv[1]).resolve()
OUT=Path(sys.argv[2]).resolve();OUT.mkdir(parents=True,exist_ok=True)
cases=[
 ('drop_entity_anchor','query_fragments.py','for position, term in self.anchors.items():','for position, term in {}.items():','tests/package_first/test_workspace.py::test_actual_typed_grounding_drives_taxon_path_not_prose'),
 ('empty_backend_result','client_api.py','bindings = self._select(query)','bindings = []','tests/package_first/test_workspace.py::test_actual_typed_grounding_drives_taxon_path_not_prose'),
 ('remove_graph_scope','query_fragments.py',"+ scope(body) +", "+ body +",'tests/package_first/test_workspace.py::test_graph_scope_keeps_connected_patterns_in_one_graph'),
 ('break_inverse','schema_models/exporters/paths.py','return f"^({parts[0]})"','return f"({parts[0]})"','tests/package_first/test_workspace.py::test_shape_only_sequence_inverse_path_is_discovered_hydrated_and_executed'),
 ('finish_on_prepare','mcp/agent.py',"name=='rdf_finish' and result.get('state') in {'complete','failed'}","name in {'rdf_prepare','rdf_finish'} and result.get('state') in {'prepared','complete','failed'}",'tests/package_first/test_mcp_flow.py::test_bridge_does_not_finish_on_prepare_or_probe_and_stops_after_explicit_final'),
 ('disable_helper_recovery','hydration.py','self.source.select_with_fallback(query, purpose="hydrate")','self.source.select(query, purpose="hydrate")','tests/package_first/test_mcp_flow.py::test_real_http_final_uses_shared_pagination_fallback_not_new_transport'),
]
results=[]
for name,module,old,new,test in cases:
 p=ROOT/'src/rdfsolve'/module;original=p.read_text()
 if old not in original:
  results.append({'mutation':name,'status':'not_applied','needle':old});continue
 try:
  p.write_text(original.replace(old,new,1))
  for cache in (ROOT/'src').rglob('__pycache__'):shutil.rmtree(cache,ignore_errors=True)
  run=subprocess.run([sys.executable,'-m','pytest','-q',test,'--disable-warnings'],cwd=ROOT,env={**os.environ,'PYTHONPATH':str(ROOT/'src')},capture_output=True,text=True,timeout=25)
  (OUT/(name+'.log')).write_text(run.stdout+run.stderr)
  results.append({'mutation':name,'test':test,'returncode':run.returncode,'status':'rejected' if run.returncode==1 else 'UNEXPECTED','summary':run.stdout.strip().splitlines()[-1:]})
 finally:
  p.write_text(original)
  for cache in (ROOT/'src').rglob('__pycache__'):shutil.rmtree(cache,ignore_errors=True)
(OUT/'summary.json').write_text(json.dumps(results,indent=2));print(json.dumps(results,indent=2))
if not all(x['status']=='rejected' for x in results):sys.exit(1)
