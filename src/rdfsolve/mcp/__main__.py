"""Run the package-backed MCP server using this Python environment."""
import argparse
import asyncio
import logging
from pathlib import Path
from rdfsolve.client_api import Client
from rdfsolve.mcp.server import run_server


def main():
    parser=argparse.ArgumentParser(description='Package-backed RDF exploration MCP (stdio)')
    parser.add_argument('--schema',type=Path,required=True)
    parser.add_argument('--source-id',default='rdf')
    parser.add_argument('--timeout',type=float,default=900)
    parser.add_argument('--max-paths',type=int,default=200)
    parser.add_argument('--data-file',type=Path)
    parser.add_argument('--log',type=Path)
    args=parser.parse_args()
    logging.basicConfig(level=logging.WARNING)
    with Client.open(args.schema, data_file=args.data_file,timeout=args.timeout) as client:
        try:
            asyncio.run(run_server(client,args.source_id,max_paths=args.max_paths))
        finally:
            if args.log:
                args.log.parent.mkdir(parents=True,exist_ok=True)
                client.save_session(args.log)


if __name__=='__main__':
    main()
