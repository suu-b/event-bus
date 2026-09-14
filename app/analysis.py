import redis
import pandas as pd
import numpy as np
import json
import argparse
from typing import Dict, List, Optional
from datetime import datetime
 

class MetricsAnalyzer:
    def __init__(self, redis_host='localhost', redis_port=6379):
        self.redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    
    def get_all_metrics(self, impl: Optional[str] = None) -> List[Dict]:
        if implementation:
           request_ids = self.redis.lrange(f"metrics:raw:{implementation}", 0, -1)
        else:
            mp_bus_ids = self.redis.lrange("metrics:raw:mp-bus", 0, -1)
            redis_bus_ids = self.redis.lrange("metrics:raw:redis-bus", 0, -1)
            request_ids = mp_bus_ids + redis_bus_ids
        
        all_metrics = []
        for req_id in request_ids:
            metrics = self.redis.hgetall(f"metrics:{req_id}");
            if metrics and 't4' in metrics:
                all_metrics.append(metrics)
    
    def calculate_perfm(self, metrics_df: pd.DataFrame) -> pd.DataFrame:
        for col in ['t0', 't1', 't2', 't3', 't4']:
            metrics_df[col] = pd.to_datetime(metrics_df[col], unit='ms')
        
        metrics_df['e2e_time'] = metrics_df['t4'] - metrics_df['t0']
        metrics_df['gateway_latency'] = metrics_df['t1'] - metrics_df['t0']
        metrics_df['queue_wait'] = metrics_df['t2'] - metrics_df['t1']
        metrics_df['processing_time'] = metrics_df['t4'] - metrics_df['t2']
        metrics_df['claim_latency'] = metrics_df['t3'] - metrics_df['t2']
        
        return metrics_df

        def generate_statistics(self, metrics_df: pd.DataFrame) -> Dict:
        stats = {}
        
        for impl in metrics_df['impl'].unique():
            impl_df = metrics_df[metrics_df['impl'] == impl]
            
            stats[impl] = {
                'total_requests': len(impl_df),
                'e2e_time': {
                    'mean_ms': impl_df['e2e_time'].mean() * 1000,
                    'median_ms': impl_df['e2e_time'].median() * 1000,
                    'p50_ms': impl_df['e2e_time'].quantile(0.5) * 1000,
                    'p95_ms': impl_df['e2e_time'].quantile(0.95) * 1000,
                    'p99_ms': impl_df['e2e_time'].quantile(0.99) * 1000,
                },
                'gateway_latency': {
                    'mean_ms': impl_df['gateway_latency'].mean() * 1000,
                    'median_ms': impl_df['gateway_latency'].median() * 1000,
                },
                'queue_wait': {
                    'mean_ms': impl_df['queue_wait'].mean() * 1000,
                    'median_ms': impl_df['queue_wait'].median() * 1000,
                },
                'processing_time': {
                    'mean_ms': impl_df['processing_time'].mean() * 1000,
                    'median_ms': impl_df['processing_time'].median() * 1000,
                }
            }
        
        return stats


        def compare_implementations(self, metrics_df: pd.DataFrame) -> Dict:
        comparison = {}
        
        mp_bus_df = metrics_df[metrics_df['impl'] == 'mp-bus']
        redis_bus_df = metrics_df[metrics_df['impl'] == 'redis-bus']
        
        metrics_to_compare = ['e2e_time', 'gateway_latency', 'queue_wait', 'processing_time']
        
        for metric in metrics_to_compare:
            if len(mp_bus_df) > 0 and len(redis_bus_df) > 0:
                mp_mean = mp_bus_df[metric].mean() * 1000
                redis_mean = redis_bus_df[metric].mean() * 1000
                
                comparison[metric] = {
                    'mp_bus_ms': mp_mean,
                    'redis_bus_ms': redis_mean,
                    'difference_ms': redis_mean - mp_mean,
                    'ratio': redis_mean / mp_mean if mp_mean > 0 else 0
                }
        
        return comparison


        def print_summary(self, stats: Dict, comparison: Dict):
        """Print a formatted summary of the analysis"""
        print("\n" + "="*50)
        print("BENCHMARKING ANALYSIS SUMMARY")
        print("="*50)
        
        for impl, impl_stats in stats.items():
            print(f"\n{impl.upper()}:")
            print(f"  Total Requests: {impl_stats['total_requests']}")
            print(f"  End-to-End Time:")
            print(f"    Mean: {impl_stats['e2e_time']['mean_ms']:.2f}ms")
            print(f"    Median: {impl_stats['e2e_time']['median_ms']:.2f}ms")
            print(f"    P95: {impl_stats['e2e_time']['p95_ms']:.2f}ms")
            print(f"    P99: {impl_stats['e2e_time']['p99_ms']:.2f}ms")
            print(f"  Gateway Latency: {impl_stats['gateway_latency']['mean_ms']:.2f}ms")
            print(f"  Queue Wait: {impl_stats['queue_wait']['mean_ms']:.2f}ms")
            print(f"  Processing Time: {impl_stats['processing_time']['mean_ms']:.2f}ms")
        
        if comparison:
            print("\n" + "-"*50)
            print("COMPARISON (redis-bus vs mp-bus):")
            print("-"*50)
            for metric, comp in comparison.items():
                print(f"{metric}:")
                print(f"  mp-bus: {comp['mp_bus_ms']:.2f}ms")
                print(f"  redis-bus: {comp['redis_bus_ms']:.2f}ms")
                print(f"  Difference: {comp['difference_ms']:.2f}ms ({comp['ratio']:.2f}x)")

def main():
    parser = argparse.ArgumentParser(description='Analyze benchmarking metrics from Redis')
    parser.add_argument('--implementation', type=str, choices=['mp-bus', 'redis-bus', 'all'], 
                        default='all', help='Implementation to analyze')
    parser.add_argument('--output', type=str, choices=['json', 'csv', 'both'], 
                        default='json', help='Output format')
    parser.add_argument('--redis-host', type=str, default='localhost', 
                        help='Redis host')
    parser.add_argument('--redis-port', type=int, default=6379, 
                        help='Redis port')
    
    args = parser.parse_args()
    
    analyzer = MetricsAnalyzer(redis_host=args.redis_host, redis_port=args.redis_port)
    
    # Get metrics
    implementation = None if args.implementation == 'all' else args.implementation
    metrics = analyzer.get_all_metrics(implementation)
    
    if not metrics:
        print("No completed metrics found in Redis")
        return
    
    # Convert to DataFrame
    metrics_df = pd.DataFrame(metrics)
    
    # Calculate performance metrics
    metrics_df = analyzer.calculate_performance_metrics(metrics_df)
    
    # Generate statistics
    stats = analyzer.generate_statistics(metrics_df)
    
    # Compare implementations if both are present
    comparison = None
    if args.implementation == 'all' and len(metrics_df['impl'].unique()) > 1:
        comparison = analyzer.compare_implementations(metrics_df)
    
    # Print summary
    analyzer.print_summary(stats, comparison if comparison else {})
    
    # Export results
    if args.output in ['json', 'both']:
        analyzer.export_results(metrics_df, stats, comparison if comparison else {}, 'json')
    if args.output in ['csv', 'both']:
        analyzer.export_results(metrics_df, stats, comparison if comparison else {}, 'csv')
 
if __name__ == '__main__':
    main()