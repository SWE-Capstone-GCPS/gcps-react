import { useRef, useEffect, useState } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import './App.css';

const INITIAL_CENTER = [-83.9921, 33.9519]; // Coordinates for Gwinnett County
const INITIAL_ZOOM = 13;

function App() {
  const mapRef = useRef(null);
  const mapContainerRef = useRef(null);
  const busMarkerRef = useRef(null);
  const wsRef = useRef(null);

  const [center, setCenter] = useState(INITIAL_CENTER);
  const [zoom, setZoom] = useState(INITIAL_ZOOM);
  const [busPosition, setBusPosition] = useState(INITIAL_CENTER);
  const [busSpeed, setBusSpeed] = useState(0);

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

      // Create a DOM element for the marker
      const el = document.createElement('div');
      el.className = 'bus-marker';
      el.style.backgroundImage = 'url(https://hebbkx1anhila5yf.public.blob.vercel-storage.com/bus-i33k23ytUTsMTcfzdld0jMMkEOtT6D.png)';
      el.style.width = '40px';
      el.style.height = '40px';
      el.style.backgroundSize = 'cover';

      // Add marker to the map
      busMarkerRef.current = new mapboxgl.Marker(el)
        .setLngLat(busPosition)
        .addTo(mapRef.current);

      console.log('Bus marker added');
    });

    return () => {
      if (mapRef.current) {
        mapRef.current.remove();
      }
    };
  }, []);

  useEffect(() => {
    // Simulating WebSocket connection to backend
    const simulateWebSocket = () => {
      return {
        onmessage: null,
        send: () => {},
        close: () => {},
      };
    };

    wsRef.current = simulateWebSocket();

    const handleMessage = (event) => {
      const data = JSON.parse(event.data);
      if (data.type === 'asset_location') {
        setBusPosition([data.longitude, data.latitude]);
      } else if (data.type === 'asset_speed') {
        setBusSpeed(data.speed);
      }
    };

    wsRef.current.onmessage = handleMessage;

    // Simulate receiving messages
    const interval = setInterval(() => {
      if (wsRef.current && wsRef.current.onmessage) {
        wsRef.current.onmessage({ data: JSON.stringify({
          type: 'asset_location',
          longitude: busPosition[0] + (Math.random() - 0.5) * 0.0001,
          latitude: busPosition[1] + (Math.random() - 0.5) * 0.0001
        })});
        wsRef.current.onmessage({ data: JSON.stringify({
          type: 'asset_speed',
          speed: Math.random() * 30
        })});
      }
    }, 1000);

    return () => {
      clearInterval(interval);
      if (wsRef.current) {
        wsRef.current.close();
      }
    };
  }, [busPosition]);

  useEffect(() => {
    if (busMarkerRef.current) {
      busMarkerRef.current.setLngLat(busPosition);
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
    <>
      <div className="sidebar">
        Longitude: {busPosition[0].toFixed(4)} | Latitude: {busPosition[1].toFixed(4)} | Zoom: {zoom.toFixed(2)}
        <br />
        Bus Speed: {busSpeed.toFixed(2)} mph
      </div>
      <button className="reset-button" onClick={handleButtonClick}>
        Reset
      </button>
      <div id="map-container" ref={mapContainerRef} style={{ width: '100vw', height: '100vh' }} />
    </>
  );
}

export default App;