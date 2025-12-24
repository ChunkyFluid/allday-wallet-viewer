import fetch from 'node-fetch';

async function getCurrentBlock() {
    try {
        const response = await fetch('https://rest-mainnet.onflow.org/v1/blocks?height=136800000');
        const data = await response.json();
        console.log('Current Block:', data[0].header.height);
        console.log('Timestamp:', data[0].header.timestamp);
    } catch (err) {
        console.error('Error:', err.message);
    }
}

getCurrentBlock();
