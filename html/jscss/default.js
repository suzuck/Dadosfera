var map;

function initMap(){
	map = new google.maps.Map(document.getElementById('mapa'), {
        zoom: 4
	});
	
	var bounds = new google.maps.LatLngBounds();
	for(i=0;i<marcadores.length;i++){
		bounds.extend(new google.maps.LatLng(marcadores[i][0],marcadores[i][1]));
		
		$('#instalacoes').html();
		
		var marc = new google.maps.Marker({
			position: new google.maps.LatLng(marcadores[i][0],marcadores[i][1])
		});
		marc.setMap(map);
	}
	map.fitBounds(bounds);
	
}
