import { useRef, useEffect, useState } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import './App.css';

const INITIAL_CENTER = [-83.9921, 33.9519]; // Coordinates for Gwinnett County
const INITIAL_ZOOM = 13;

// Route for BUS-001: Lawrenceville to Duluth
const BUS_ROUTE = [
  { lat: 33.9562, lng: -83.9879 }, // Lawrenceville Square
  { lat: 33.9584, lng: -83.9925 }, // W Crogan St
  { lat: 33.9619, lng: -84.0024 }, // GA-20 W
  { lat: 33.9704, lng: -84.0270 }, // Buford Dr NW
  { lat: 33.9736, lng: -84.0718 }, // Pleasant Hill Rd
  { lat: 33.9696, lng: -84.0947 }, // Duluth Hwy
  { lat: 33.9592, lng: -84.1118 }, // Duluth Town Green
];

function App() {
  const mapRef = useRef(null);
  const mapContainerRef = useRef(null);
  const busMarkerRef = useRef(null);
  const animationRef = useRef(null);

  const [center, setCenter] = useState(INITIAL_CENTER);
  const [zoom, setZoom] = useState(INITIAL_ZOOM);
  const [busPosition, setBusPosition] = useState(BUS_ROUTE[0]);
  const [busSpeed, setBusSpeed] = useState(0);
  const [routeIndex, setRouteIndex] = useState(0);

  useEffect(() => {
    mapboxgl.accessToken = 'pk.eyJ1Ijoic2FyYWhmYXNoaW5hc2kiLCJhIjoiY20xczg0cWRyMDNtOTJsb2R6cmNiZmRyNyJ9.Utvb8kECGGDYQljL0fknfA';
    
    if (!mapContainerRef.current) {
      console.error('Map container ref is null');
      return;
    }

    mapRef.current = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/streets-v11',
      center: [busPosition.lng, busPosition.lat],
      zoom: zoom
    });

    mapRef.current.on('load', () => {
      console.log('Map loaded');

      if (!mapRef.current) {
        console.error('Map reference is null');
        return;
      }

      // Create a DOM element for the marker
      const el = document.createElement('div');
      el.className = 'bus-marker';
      el.style.backgroundImage = 'url(https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bus-i33k23ytUTsMTcfzdld0jMMkEOtT6D.png)';
      el.style.width = '40px';
      el.style.height = '40px';
      el.style.backgroundSize = 'cover';

      // Add marker to the map
      busMarkerRef.current = new mapboxgl.Marker(el)
        .setLngLat([busPosition.lng, busPosition.lat])
        .addTo(mapRef.current);

      console.log('Bus marker added');

      // Add the route to the map
      mapRef.current.addSource('route', {
        'type': 'geojson',
        'data': {
          'type': 'Feature',
          'properties': {},
          'geometry': {
            'type': 'LineString',
            'coordinates': BUS_ROUTE.map(point => [point.lng, point.lat])
          }
        }
      });

      mapRef.current.addLayer({
        'id': 'route',
        'type': 'line',
        'source': 'route',
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

    return () => {
      if (mapRef.current) {
        mapRef.current.remove();
      }
    };
  }, []);

  useEffect(() => {
    // Simulated WebSocket
    const simulateWebSocket = () => {
      const nextIndex = (routeIndex + 1) % BUS_ROUTE.length;
      const nextPosition = BUS_ROUTE[nextIndex];
      const speed = Math.random() * 30 + 10; // Random speed between 10 and 40 mph

      setBusPosition(nextPosition);
      setBusSpeed(speed);
      setRouteIndex(nextIndex);

      // Move the map center to follow the bus
      if (mapRef.current) {
        mapRef.current.setCenter([nextPosition.lng, nextPosition.lat]);
      }
    };

    // Simulate WebSocket updates every 5 seconds
    const intervalId = setInterval(simulateWebSocket, 5000);

    return () => clearInterval(intervalId);
  }, [routeIndex]);

  useEffect(() => {
    if (busMarkerRef.current) {
      busMarkerRef.current.setLngLat([busPosition.lng, busPosition.lat]);
      console.log('Bus marker position updated:', busPosition);
    }
  }, [busPosition]);

  const handleButtonClick = () => {
    if (mapRef.current) {
      mapRef.current.flyTo({
        center: INITIAL_CENTER,
        zoom: INITIAL_ZOOM
      });
    }
  };

  return (
    <div className="app-container">
      <div className="sidebar">
        <h2 className="sidebar-title">Bus Information</h2>
        <div className="bus-details">
          <p>Longitude: {busPosition.lng.toFixed(4)}</p>
          <p>Latitude: {busPosition.lat.toFixed(4)}</p>
          <p>Speed: {busSpeed.toFixed(2)} mph</p>
          <p>Zoom: {zoom.toFixed(2)}</p>
        </div>
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
