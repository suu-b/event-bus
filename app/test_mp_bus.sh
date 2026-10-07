
echo "🚀 Starting Scalability Test for MP Bus Impl"
echo "=================================="

# Test different request counts
REQUEST_COUNTS=(10)

for count in "${REQUEST_COUNTS[@]}"; do
    echo ""
    echo "📊 Testing with $count requests..."
    
    # Clear Redis
    redis-cli FLUSHDB > /dev/null 2>&1
    
    # Submit requests
    python mock_client.py --count $count --parallel --workers 10
    
    # Wait for completion
    python monitor.py --wait --interval 2 --timeout 180
    
    # Small delay to ensure metrics are fully recorded
    sleep 2
    
    # Analyze results
    python analysis.py --implementation mp-bus > scalability_mp_$count.log 2>&1
    
    echo "✅ Completed $count requests"
done

echo ""
echo "🎯 Scalability analysis complete!"
echo "Results saved as scalability_*.csv files"