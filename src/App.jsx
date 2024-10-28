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
  const [busData, setBusData] = useState([]);

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
    });

      return () => {
        if (mapRef.current) {
          mapRef.current.remove();
        }
      };
    }, []);
  
    useEffect(() => {
      wsRef.current = new WebSocket('ws://localhost:8765');
  
      wsRef.current.onmessage = (event) => {
        const data = JSON.parse(event.data);
        setBusData(data);
      };
  
      return () => {
        if (wsRef.current) {
          wsRef.current.close();
        }
      };
    }, []);
  
    useEffect(() => {
      if (!mapRef.current) return;
  
      busData.forEach((bus) => {
        if (!busMarkersRef.current[bus.asset_id]) {
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

      } else {
        busMarkersRef.current[bus.asset_id].setLngLat([bus.longitude, bus.latitude]);
      }
    });
  }, [busData]);

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
        {busData.map((bus) => (
          <div key={bus.asset_id}>
            Bus {bus.asset_id}: Lat {bus.latitude.toFixed(4)}, Lon {bus.longitude.toFixed(4)}, Speed {bus.speed ? bus.speed.toFixed(2) : 'N/A'} mph
          </div>
        ))}
      </div>
      <button className="reset-button" onClick={handleButtonClick}>
        Reset
      </button>
      <div id="map-container" ref={mapContainerRef} style={{ width: '100vw', height: '100vh' }} />
    </>
  );
}

export default App;