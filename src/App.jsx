import { useRef, useEffect, useState } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import './App.css';

const INITIAL_CENTER = [-84.0, 33.96]; // Coordinates for Gwinnett County
const INITIAL_ZOOM = 10.12;

function App() {
  const mapRef = useRef();
  const mapContainerRef = useRef();

  const [center, setCenter] = useState(INITIAL_CENTER);
  const [zoom, setZoom] = useState(INITIAL_ZOOM);
  const [buses, setBuses] = useState([]);

  useEffect(() => {
    mapboxgl.accessToken = 'pk.eyJ1Ijoic2FyYWhmYXNoaW5hc2kiLCJhIjoiY20xczg0cWRyMDNtOTJsb2R6cmNiZmRyNyJ9.Utvb8kECGGDYQljL0fknfA';
    
    mapRef.current = new mapboxgl.Map({
      container: mapContainerRef.current,
      style: 'mapbox://styles/mapbox/streets-v11',
      center: center,
      zoom: zoom
    });

    // Update state on map move
    mapRef.current.on('move', () => {
      const mapCenter = mapRef.current.getCenter();
      const mapZoom = mapRef.current.getZoom();

      // Update state with current map center and zoom
      setCenter([mapCenter.lng, mapCenter.lat]);
      setZoom(mapZoom);
    });

    mapRef.current.on('load', () => {
      mapRef.current.addSource('buses', {
        type: 'geojson',
        data: {
          type: 'FeatureCollection',
          features: []
        }
      });

      mapRef.current.addLayer({
        id: 'buses',
        type: 'symbol',
        source: 'buses',
        layout: {
          'icon-image': 'bus-15',
          'icon-size': 1.5,
          'text-field': ['get', 'speed'],
          'text-font': ['Open Sans Semibold', 'Arial Unicode MS Bold'],
          'text-offset': [0, 0.6],
          'text-anchor': 'top'
        }
      });
    });

    return () => {
      mapRef.current.remove();
    };
  }, []); 

  useEffect(() => {
    const fetchBuses = async () => {
      try {
        const response = await fetch('/api/buses');
        const data = await response.json();
        setBuses(data);
      } catch (error) {
        console.error('Error fetching bus data:', error);
      }
    };

    fetchBuses();
    const interval = setInterval(fetchBuses, 5000); // Update every 5 seconds

    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (!mapRef.current || !mapRef.current.getSource('buses')) return;

    const features = buses.map(bus => ({
      type: 'Feature',
      geometry: {
        type: 'Point',
        coordinates: [bus.longitude, bus.latitude]
      },
      properties: {
        id: bus.id,
        speed: `${bus.speed.toFixed(1)} mph`
      }
    }));

    mapRef.current.getSource('buses').setData({
      type: 'FeatureCollection',
      features
    });
  }, [buses]);

  // Reset the map center and zoom
  const handleButtonClick = () => {
    mapRef.current.flyTo({
      center: INITIAL_CENTER,
      zoom: INITIAL_ZOOM
    });
  };

  return (
    <>
      <div className="sidebar">
        Longitude: {center[0].toFixed(4)} | Latitude: {center[1].toFixed(4)} | Zoom: {zoom.toFixed(2)}
      </div>
      <button className="reset-button" onClick={handleButtonClick}>
        Reset
      </button>
      <div id="map-container" ref={mapContainerRef} style={{ width: '100vw', height: '100vh' }} />
    </>
  );
}

export default App;