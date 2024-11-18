import React, { useRef, useEffect, useState } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import './App.css';

const INITIAL_CENTER = [-83.9921, 33.9519]; // Coordinates for Gwinnett County
const INITIAL_ZOOM = 10;

const BUS_ROUTES = {
  'BUS-001': [
    { lat: 33.9562, lng: -83.9879 }, // Lawrenceville Square
    { lat: 33.9584, lng: -83.9925 }, // W Crogan St
    { lat: 33.9619, lng: -84.0024 }, // GA-20 W
    { lat: 33.9704, lng: -84.0270 }, // Buford Dr NW
    { lat: 33.9736, lng: -84.0718 }, // Pleasant Hill Rd
    { lat: 33.9696, lng: -84.0947 }, // Duluth Hwy
    { lat: 33.9592, lng: -84.1118 }, // Duluth Town Green
  ],
  'BUS-002': [
    { lat: 33.8578, lng: -84.0199 }, // Snellville City Hall
    { lat: 33.8615, lng: -84.0233 }, // Wisteria Dr SW
    { lat: 33.8682, lng: -84.0430 }, // US-78 W
    { lat: 33.8860, lng: -84.0930 }, // Stone Mountain Hwy
    { lat: 33.8879, lng: -84.1429 }, // Killian Hill Rd
    { lat: 33.8903, lng: -84.1482 }, // Lilburn City Hall
  ],
  'BUS-003': [
    { lat: 34.0515, lng: -84.0712 }, // Suwanee Town Center
    { lat: 34.0598, lng: -84.0741 }, // Lawrenceville-Suwanee Rd
    { lat: 34.0805, lng: -84.0778 }, // I-85 N
    { lat: 34.1205, lng: -84.0044 }, // I-985 N
    { lat: 34.1207, lng: -83.9911 }, // Buford Dr NE
    { lat: 34.1207, lng: -83.9911 }, // Buford Town Center
  ],
  'BUS-004': [
    { lat: 33.9412, lng: -84.2135 }, // Norcross City Hall
    { lat: 33.9470, lng: -84.2179 }, // Buford Hwy NE
    { lat: 33.9613, lng: -84.2245 }, // Jimmy Carter Blvd
    { lat: 33.9695, lng: -84.2336 }, // Peachtree Industrial Blvd
    { lat: 33.9695, lng: -84.2336 }, // Peachtree Corners Circle
    { lat: 33.9695, lng: -84.2336 }, // Peachtree Corners Town Center
  ],
  'BUS-005': [
    { lat: 33.9887, lng: -83.8977 }, // Dacula City Hall
    { lat: 33.9933, lng: -83.8915 }, // Dacula Rd
    { lat: 34.0070, lng: -83.8698 }, // GA-8 E
    { lat: 34.0161, lng: -83.8327 }, // GA-324 E
    { lat: 34.0190, lng: -83.8285 }, // Auburn Rd
    { lat: 34.0190, lng: -83.8285 }, // Auburn City Hall
  ],
};

function App() {
  const mapRef = useRef(null);
  const mapContainerRef = useRef(null);
  const busMarkersRef = useRef({});

  const [center, setCenter] = useState(INITIAL_CENTER);
  const [zoom, setZoom] = useState(INITIAL_ZOOM);
  const [busPositions, setBusPositions] = useState(
    Object.fromEntries(Object.entries(BUS_ROUTES).map(([id, route]) => [id, route[0]]))
  );
  const [busSpeeds, setBusSpeeds] = useState(
    Object.fromEntries(Object.keys(BUS_ROUTES).map(id => [id, 0]))
  );
  const [routeIndices, setRouteIndices] = useState(
    Object.fromEntries(Object.keys(BUS_ROUTES).map(id => [id, 0]))
  );
  const [selectedBus, setSelectedBus] = useState(null);

  useEffect(() => {
    mapboxgl.accessToken = 'pk.eyJ1Ijoic2FyYWhmYXNoaW5hc2kiLCJhIjoiY20xczg0cWRyMDNtOTJsb2R6cmNiZmRyNyJ9.Utvb8kECGGDYQljL0fknfA';
    
    if (!mapContainerRef.current) {
      console.error('Map container ref is null');
      return;
    }

    mapRef.current = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/streets-v11',
      center: center,
      zoom: zoom
    });

    mapRef.current.on('load', () => {
      console.log('Map loaded');

      if (!mapRef.current) {
        console.error('Map reference is null');
        return;
      }

      // Add bus markers and routes
      Object.entries(BUS_ROUTES).forEach(([busId, route]) => {
        // Create a DOM element for the marker
        const el = document.createElement('div');
        el.className = 'bus-marker';
        el.style.backgroundImage = 'url(https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bus-i33k23ytUTsMTcfzdld0jMMkEOtT6D.png)';
        el.style.width = '40px';
        el.style.height = '40px';
        el.style.backgroundSize = 'cover';

        // Add marker to the map
        busMarkersRef.current[busId] = new mapboxgl.Marker(el)
          .setLngLat([route[0].lng, route[0].lat])
          .addTo(mapRef.current);

        // Add the route to the map
        mapRef.current.addSource(`route-${busId}`, {
          'type': 'geojson',
          'data': {
            'type': 'Feature',
            'properties': {},
            'geometry': {
              'type': 'LineString',
              'coordinates': route.map(point => [point.lng, point.lat])
            }
          }
        });

        mapRef.current.addLayer({
          'id': `route-${busId}`,
          'type': 'line',
          'source': `route-${busId}`,
          'layout': {
            'line-join': 'round',
            'line-cap': 'round'
          },
          'paint': {
            'line-color': '#888',
            'line-width': 2
          }
        });
      });
    });

    return () => {
      if (mapRef.current) {
        mapRef.current.remove();
      }
    };
  }, []);

  useEffect(() => {
    // Simulated WebSocket
    const simulateWebSocket = () => {
      const newPositions = { ...busPositions };
      const newSpeeds = { ...busSpeeds };
      const newIndices = { ...routeIndices };

      Object.keys(BUS_ROUTES).forEach(busId => {
        const nextIndex = (newIndices[busId] + 1) % BUS_ROUTES[busId].length;
        const nextPosition = BUS_ROUTES[busId][nextIndex];
        const speed = Math.random() * 30 + 10; // Random speed between 10 and 40 mph

        newPositions[busId] = nextPosition;
        newSpeeds[busId] = speed;
        newIndices[busId] = nextIndex;
      });

      setBusPositions(newPositions);
      setBusSpeeds(newSpeeds);
      setRouteIndices(newIndices);
    };

    // Simulate WebSocket updates every 5 seconds
    const intervalId = setInterval(simulateWebSocket, 5000);

    return () => clearInterval(intervalId);
  }, [busPositions, busSpeeds, routeIndices]);

  useEffect(() => {
    Object.entries(busPositions).forEach(([busId, position]) => {
      if (busMarkersRef.current[busId]) {
        busMarkersRef.current[busId].setLngLat([position.lng, position.lat]);
      }
    });
  }, [busPositions]);

  const handleButtonClick = () => {
    if (mapRef.current) {
      mapRef.current.flyTo({
        center: INITIAL_CENTER,
        zoom: INITIAL_ZOOM
      });
    }
  };

  const handleBusClick = (busId) => {
    setSelectedBus(busId);
    if (mapRef.current && busPositions[busId]) {
      mapRef.current.flyTo({
        center: [busPositions[busId].lng, busPositions[busId].lat],
        zoom: 14
      });
    }
  };

  return (
    <div className="app-container">
      <div className="sidebar">
        <h2 className="sidebar-title">Bus Information</h2>
        {Object.keys(BUS_ROUTES).map(busId => (
          <div key={busId} className="bus-details" onClick={() => handleBusClick(busId)}>
            <h3>{busId}</h3>
            <p>Longitude: {busPositions[busId]?.lng.toFixed(4)}</p>
            <p>Latitude: {busPositions[busId]?.lat.toFixed(4)}</p>
            <p>Speed: {busSpeeds[busId]?.toFixed(2)} mph</p>
          </div>
        ))}
      </div>
      <div className="map-wrapper">
        <button className="reset-button" onClick={handleButtonClick}>
          Reset View
        </button>
        <div id="map-container" ref={mapContainerRef} className="map-container" />
      </div>
    </div>
  );
}

export default App;