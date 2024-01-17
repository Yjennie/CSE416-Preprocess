import json
import os
import sys

from gerrychain import (GeographicPartition, Graph, MarkovChain,
						updaters, constraints, accept)
from gerrychain.proposals import recom
from shapely.wkt import loads

import geopandas
import pandas
import functools
import shapely
import re
import threading


def main():
	graph = Graph.from_file("florida.geojson")
	my_updaters = {
		"population": updaters.Tally("TOTPOP20", alias="population")
	}
	initial_partition = GeographicPartition(graph,
											assignment="CONG_DIST",
											updaters=my_updaters)
	ideal_population = sum(initial_partition["population"].values()) / len(initial_partition)
	proposal = functools.partial(recom,
								 pop_col="TOTPOP20",
								 pop_target=ideal_population,
								 epsilon=0.02,
								 node_repeats=2)
	population_constraint = constraints.within_percent_of_ideal_population(initial_partition, 0.2)
	compactness_bound = constraints.UpperBound(
		lambda p: len(p["cut_edges"]),
		2 * len(initial_partition["cut_edges"])
	)

	# Check if the directory has enough plans. Run recom however many times until we reach the desired plan number.
	files = os.listdir("./florida_plans")
	iteration = 0
	initial_state = initial_partition

	while True:
		geojson_files = [0 for file in files if file.endswith('.json')]
		if len(geojson_files) > ensemble_size:
			print("Ensemble size has reached limit.")
			break
		chain = MarkovChain(proposal=proposal,
							constraints=[
								population_constraint,
								compactness_bound
							],
							accept=accept.always_accept,
							initial_state=initial_state,
							total_steps=step_size
							)
		initial_state = run_recom(iteration, chain)  # returns the last district plan in the chain
		iteration += 1


def number_of_cut_edges(partition):
	return len(partition["cut_edges"])


def run_recom(iteration, chain):
	partitions = [partition for partition in chain.with_progress_bar()]
	targ_partition = partitions[-1]
	parts = targ_partition.parts

	# Convert the FrozenGraph object into a dictionary
	graph_dict = {}
	for node in targ_partition.graph.nodes:
		graph_dict[node] = targ_partition.graph.nodes[node]

	# Now you can create the NetworkX graph
	for district_id, nodes in parts.items():
		for node in nodes:
			graph_dict[node]['district_id'] = district_id
			graph_dict[node].pop("boundary_node", None)
			graph_dict[node].pop("geometry", None)

	with open(f"./florida_plans/new_florida_{threading.get_ident()}_{iteration}.json", "w") as f:
		json.dump(graph_dict, f)

	return targ_partition


def run_recom_merge(iteration, chain):
	partitions = [partition for partition in chain.with_progress_bar()]
	# Sum up election and demographic data
	parts = partitions[-1].parts

	# Convert parts into a GeoDataFrame
	district_data = []
	# Loop through parts (districts)
	for district_id, nodes in parts.items():
		# Get the geometries of the nodes in each district
		geometries = [partitions[-1].graph.nodes[node]["geometry"] for node in nodes]

		# Create a Polygon that represents the district
		district_geometry = shapely.unary_union(geometries)

		# Sum up election and demographic data
		exclude_keys = ["NAME20", "CONG_DIST", "boundary_node", "geometry", "boundary_perim"]
		unsummed_district_data = []
		for node in nodes:
			unsummed_district_data.append(partitions[-1].graph.nodes[node])
		district_metadata = {key: sum(int(d[key]) for d in unsummed_district_data if key not in exclude_keys) for key in
							 set().union(*unsummed_district_data)}

		# Create a GeoDataFrame for the new district plan
		district_metadata["district_id"] = district_id
		district_df = pandas.DataFrame(district_metadata, index=[district_id])
		district_gdf = geopandas.GeoDataFrame(district_df, geometry=[district_geometry], crs=32030)
		district_data.append(district_gdf)

		districts_gdf = geopandas.GeoDataFrame(pandas.concat(district_data), crs=32030)
		cleaned_districts = districts_gdf.drop(columns=["boundary_node",
														"CONG_DIST",
														"NAME20"])

		# Round all coordinates to four decimal places
		coord_pattern = re.compile(r"\d*\.\d+")

		def polygon_round(match):
			return "{:.4f}".format(float(match.group()))

		cleaned_districts.geometry = cleaned_districts.geometry.apply(
			lambda x: loads(re.sub(coord_pattern, polygon_round, x.wkt)))

		with open(f"./florida_plans/new_florida_{threading.get_ident()}_{iteration}.geojson", "w") as f:
			f.write(cleaned_districts.to_json())


if __name__ == "__main__":
	ensemble_size = int(sys.argv[1])
	step_size = int(sys.argv[2])

	threads = []

	for i in range(0, 10):
		t = threading.Thread(target=main)
		t.start()
		threads.append(t)

	for thread in threads:
		thread.join()

