
echo "🚀 Starting Scalability Test for Redis Bus Impl"
echo "=================================="

# Test different request counts
REQUEST_COUNTS=(10 20 50 100)

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
    python analysis.py --implementation redis-bus > scalability_redis_$count.log 2>&1
    
    echo "✅ Completed $count requests"
done

echo ""
echo "🎯 Scalability analysis complete!"
echo "Results saved as scalability_*.csv files"