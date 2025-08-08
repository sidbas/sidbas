pip install streamlit-d3graph
import streamlit as st
from streamlit_d3graph import d3graph

d3 = d3graph()
d3.graph({'A': ['B', 'C'], 'B': ['D'], 'C': ['D']})
d3.show()


pip install networkx pyvis

from pyvis.network import Network
import networkx as nx
import streamlit as st

G = nx.DiGraph()
G.add_edges_from([("Extract", "Transform"), ("Transform", "Load")])

net = Network(notebook=False, directed=True)
net.from_nx(G)

net.show("lineage_graph.html")

with open("lineage_graph.html", 'r', encoding='utf-8') as f:
    html = f.read()

st.components.v1.html(html, height=600, scrolling=True)

pip install networkx pyvis
from pyvis.network import Network
import networkx as nx
import streamlit as st

G = nx.DiGraph()
G.add_edges_from([("Extract", "Transform"), ("Transform", "Load")])

net = Network(notebook=False, directed=True)
net.from_nx(G)

net.show("lineage_graph.html")

with open("lineage_graph.html", 'r', encoding='utf-8') as f:
    html = f.read()

st.components.v1.html(html, height=600, scrolling=True)

Show your field mappings in AgGrid
import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridUpdateMode

# Sample source-target mappings
df = pd.DataFrame({
    'source_field': ['customer.id', 'customer.name', 'order.id', 'order.amount'],
    'target_field': ['dim_customer.customer_id', 'dim_customer.name', 'fact_order.order_id', 'fact_order.total']
})

# Show it in AgGrid
grid_response = AgGrid(
    df,
    editable=False,
    height=200,
    update_mode=GridUpdateMode.NO_UPDATE,
    fit_columns_on_grid_load=True
)

# Use the data from AgGrid (in case of any edits in future)
data = grid_response['data']

Create a graph from that mapping (e.g. using pyvis)
from pyvis.network import Network
import networkx as nx

# Extract edges
edges = list(zip(data['source_field'], data['target_field']))

# Create a directed graph
G = nx.DiGraph()
G.add_edges_from(edges)

# Use pyvis for interactive display
net = Network(height='500px', directed=True, notebook=False)
net.from_nx(G)

# Save to HTML and render in Streamlit
net.show('lineage.html')

with open("lineage.html", "r", encoding='utf-8') as f:
    html_string = f.read()

st.components.v1.html(html_string, height=550, scrolling=True)

Alternative) Using streamlit-d3graph for native DAG rendering
from streamlit_d3graph import d3graph

# Build dictionary from source-target mappings
edges_dict = {}
for s, t in edges:
    edges_dict.setdefault(s, []).append(t)

# Create and show graph
d3 = d3graph()
d3.graph(edges_dict)
d3.show()



