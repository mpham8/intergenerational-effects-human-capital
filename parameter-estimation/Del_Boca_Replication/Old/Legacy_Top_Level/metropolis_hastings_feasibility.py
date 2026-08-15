import time
import numpy as np
import os
import concurrent.futures
import matplotlib.pyplot as plt
from typing import List, Dict, Any
import solve_system  # Your solver module
from Del_Boca_Implementation import create_households, solve_households



def simple_household_speed_test(parameters: List, n_test_households: int = 50) -> Dict[str, Any]:
    """
    Simple speed test that avoids nested multiprocessing issues.
    Tests single household performance and uses your existing solve_households function.
    
    Parameters:
    -----------
    parameters : List
        Your parameter list for the solver
    n_test_households : int
        Number of test households to create and solve
        
    Returns:
    --------
    Dict : Speed test results and MH feasibility assessment
    """
    
    print("="*70)
    print("SIMPLIFIED HOUSEHOLD SOLVER SPEED TEST")
    print("="*70)
    print(f"Testing with {n_test_households} households")
    print(f"Parameter vector length: {len(parameters)}")
    
    # Create test households using your function
    print("\n1. Creating test households...")
    start_time = time.time()
    test_households = create_households(n_test_households)
    creation_time = time.time() - start_time
    print(f"   Household creation: {creation_time:.3f} seconds")
    print(f"   Household array shape: {test_households.shape}")
    
    results = {
        'creation_time': creation_time,
        'n_households': n_test_households,
        'n_parameters': len(parameters)
    }
    
    # Test 1: Single household timing (most important for MH)
    print("\n2. Testing single household solve times...")
    single_times = []
    failed_count = 0
    
    # Test 100 individual households
    for i in range(min(100, n_test_households)):
        start_time = time.time()
        try:
            solved = solve_system.solve_household(parameters, test_households[i])
            solve_time = time.time() - start_time
            single_times.append(solve_time)
            print(f"   Household {i+1}: {solve_time:.4f} seconds")
        except Exception as e:
            solve_time = time.time() - start_time
            failed_count += 1
            print(f"   Household {i+1}: FAILED after {solve_time:.4f}s - {str(e)[:50]}...")
            single_times.append(np.nan)  # Keep track of failures
    
    if not any(~np.isnan(single_times)):
        print("   ERROR: No households solved successfully!")
        return {'error': 'All single household solves failed'}
    
    # Calculate statistics for successful solves only
    valid_times = [t for t in single_times if not np.isnan(t)]
    single_stats = {
        'mean': np.mean(valid_times),
        'std': np.std(valid_times),
        'min': np.min(valid_times),
        'max': np.max(valid_times),
        'median': np.median(valid_times),
        'success_rate': len(valid_times) / len(single_times),
        'all_times': valid_times
    }
    results['single_household'] = single_stats
    
    print(f"\n   Single Household Statistics:")
    print(f"   • Mean: {single_stats['mean']:.4f} ± {single_stats['std']:.4f} seconds")
    print(f"   • Range: [{single_stats['min']:.4f}, {single_stats['max']:.4f}] seconds") 
    print(f"   • Median: {single_stats['median']:.4f} seconds")
    print(f"   • Success rate: {len(valid_times)}/{len(single_times)} ({single_stats['success_rate']:.1%})")
    
    # Test 2: Your existing multiprocessing function
    print(f"\n3. Testing your solve_households function...")
    
    # Test with different core counts using your existing function
    test_cores = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
    available_cores = os.cpu_count()
    
    batch_results = {}
    
    for num_cores in test_cores:
        if num_cores > available_cores:
            print(f"   Skipping {num_cores} cores (only {available_cores} available)")
            continue
            
        print(f"\n   Testing with {num_cores} cores...")
        
        # Create fresh copy for this test
        test_batch = test_households.copy()
        
        start_time = time.time()
        try:
            # Call your function directly - it handles multiprocessing internally
            solved_households = solve_households(test_batch, parameters, num_workers=num_cores)
            
            total_time = time.time() - start_time
            
            # Count successful solves (households with non-zero solutions)
            successful_solves = 0
            for i in range(n_test_households):
                if np.any(solved_households[i, :, 3:] != 0):
                    successful_solves += 1
            
            batch_results[num_cores] = {
                'total_time': total_time,
                'successful_solves': successful_solves,
                'households_per_second': successful_solves / total_time if total_time > 0 else 0,
                'success_rate': successful_solves / n_test_households,
                'time_per_household': total_time / n_test_households,
                'efficiency': (successful_solves / total_time) / (successful_solves / batch_results.get(1, {}).get('total_time', total_time)) if 1 in batch_results else 1.0
            }
            
            print(f"      Total time: {total_time:.2f} seconds")
            print(f"      Successful solves: {successful_solves}/{n_test_households}")
            print(f"      Throughput: {successful_solves/total_time:.2f} households/second")
            print(f"      Time per household: {total_time/n_test_households:.4f} seconds")
            
            if 1 in batch_results and num_cores > 1:
                speedup = batch_results[1]['total_time'] / total_time
                efficiency = speedup / num_cores
                print(f"      Speedup vs 1 core: {speedup:.2f}x")
                print(f"      Parallel efficiency: {efficiency:.1%}")
            
        except Exception as e:
            print(f"      FAILED: {str(e)}")
            batch_results[num_cores] = {'error': str(e)}
    
    results['batch_processing'] = batch_results
    
    # MH Feasibility Analysis
    print(f"\n" + "="*70)
    print("METROPOLIS-HASTINGS FEASIBILITY ASSESSMENT")
    print("="*70)
    
    avg_single_time = single_stats['mean']
    full_dataset_size = 11500
    
    print(f"\nScaling to full dataset ({full_dataset_size:,} households):")
    print(f"• Single household time: {avg_single_time:.4f} seconds")
    
    # Sequential estimate
    sequential_full_time = avg_single_time * full_dataset_size
    print(f"• Sequential full likelihood: {sequential_full_time/60:.1f} minutes")
    print(f"                              ({sequential_full_time/3600:.2f} hours)")
    
    # Find best parallel performance
    best_cores = None
    best_time_per_hh = float('inf')
    best_throughput = 0
    
    for cores, stats in batch_results.items():
        if 'error' not in stats and stats['success_rate'] > 0.8:  # Only consider high success rates
            if stats['time_per_household'] < best_time_per_hh:
                best_time_per_hh = stats['time_per_household']
                best_cores = cores
                best_throughput = stats['households_per_second']
    
    if best_cores:
        parallel_full_time = best_time_per_hh * full_dataset_size
        speedup = sequential_full_time / parallel_full_time
        
        print(f"\nWith optimal multiprocessing ({best_cores} cores):")
        print(f"• Time per household: {best_time_per_hh:.4f} seconds") 
        print(f"• Full likelihood time: {parallel_full_time/60:.1f} minutes")
        print(f"                        ({parallel_full_time/3600:.2f} hours)")
        print(f"• Speedup: {speedup:.1f}x")
        print(f"• Throughput: {best_throughput:.1f} households/second")
        
        # MH iteration estimates
        one_week_seconds = 7 * 24 * 3600
        feasible_iterations = int(one_week_seconds / parallel_full_time)
        
        print(f"\nMH FEASIBILITY:")
        print(f"• Max iterations in 1 week: {feasible_iterations:,}")
        
        # Recommendations based on feasible iterations
        if feasible_iterations >= 10000:
            status = "✓ EXCELLENT"
            color = "green"
            recommendations = [
                "Standard MH with full dataset should work perfectly",
                f"Use all {best_cores} cores for maximum efficiency",
                "Can easily run 10,000+ iterations for thorough exploration",
                "Consider running multiple chains for robustness"
            ]
        elif feasible_iterations >= 5000:
            status = "✓ VERY GOOD"
            color = "blue"
            recommendations = [
                "Standard MH with full dataset is feasible",
                f"Use {best_cores} cores for optimal performance",
                "5,000+ iterations should provide good convergence",
                "Monitor convergence diagnostics"
            ]
        elif feasible_iterations >= 1000:
            status = "⚠ MODERATE"
            color = "orange"
            recommendations = [
                "MH feasible but may need optimization",
                "Consider random subsets of 3,000-5,000 households per iteration",
                "Use adaptive proposals to reduce rejected samples",
                "Implement parallel tempering for better mixing"
            ]
        else:
            status = "✗ CHALLENGING"
            color = "red"
            recommendations = [
                "Standard MH may be too slow",
                "Use random subsets of 1,000-2,000 households per iteration",
                "Consider surrogate models or emulation",
                "Try variational methods or sequential estimation"
            ]
        
        print(f"• Status: {status}")
        print(f"\nRECOMMENDATIONS:")
        for i, rec in enumerate(recommendations, 1):
            print(f"  {i}. {rec}")
            
        results['feasibility'] = {
            'status': status,
            'feasible_iterations_per_week': feasible_iterations,
            'best_cores': best_cores,
            'parallel_full_time_hours': parallel_full_time / 3600,
            'recommendations': recommendations
        }
    
    else:
        print("\n⚠ Could not determine optimal parallel performance")
        results['feasibility'] = {'status': 'UNKNOWN', 'error': 'No successful parallel runs'}
    
    return results

def plot_simple_results(results: Dict[str, Any]):
    """Plot the simplified speed test results"""
    
    if 'single_household' not in results or 'batch_processing' not in results:
        print("Insufficient data for plotting")
        return
    
    batch_data = results['batch_processing']
    
    # Extract successful results
    cores = []
    throughput = []
    speedup = []
    
    baseline_time = None
    
    for core_count, stats in sorted(batch_data.items()):
        if 'error' in stats:
            continue
            
        cores.append(core_count)
        throughput.append(stats['households_per_second'])
        
        if baseline_time is None:
            baseline_time = stats['total_time']
            speedup.append(1.0)
        else:
            speedup.append(baseline_time / stats['total_time'])
    
    if len(cores) < 2:
        print("Need at least 2 successful core tests for meaningful plots")
        return
    
    # Create plots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # Plot 1: Single household timing distribution
    single_times = results['single_household']['all_times']
    ax1.hist(single_times, bins=min(10, len(single_times)), alpha=0.7, edgecolor='black')
    ax1.axvline(results['single_household']['mean'], color='red', linestyle='--', 
                label=f"Mean: {results['single_household']['mean']:.4f}s")
    ax1.set_xlabel('Solve Time (seconds)')
    ax1.set_ylabel('Frequency')
    ax1.set_title('Single Household Solve Time Distribution')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Throughput vs cores
    ax2.plot(cores, throughput, 'bo-', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Cores')
    ax2.set_ylabel('Households per Second')
    ax2.set_title('Solver Throughput vs CPU Cores')
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Speedup vs cores  
    ax3.plot(cores, speedup, 'ro-', linewidth=2, markersize=8, label='Actual')
    ax3.plot(cores, cores, 'k--', alpha=0.5, label='Linear speedup')
    ax3.set_xlabel('Number of Cores')
    ax3.set_ylabel('Speedup Factor')
    ax3.set_title('Parallel Speedup vs CPU Cores')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: MH Feasibility
    # if 'feasibility' in results and 'feasible_iterations_per_week' in results['feasibility']:
    #     iterations = results['feasibility']['feasible_iterations_per_week']
        
    #     # Create a simple bar chart showing feasibility
    #     categories = ['Feasible\nIterations\n(1 week)', 'Minimum\nRecommended\n(5,000)', 'Ideal\nTarget\n(10,000)']
    #     values = [iterations, 5000, 10000]
    #     colors = ['blue', 'orange', 'green']
        
    #     bars = ax4.bar(categories, values, color=colors, alpha=0.7)
    #     ax4.set_ylabel('Number of Iterations')
    #     ax4.set_title('MH Feasibility Assessment')
    #     ax4.grid(True, alpha=0.3, axis='y')
        
    #     # Add value labels on bars
    #     for bar, value in zip(bars, values):
    #         height = bar.get_height()
    #         ax4.text(bar.get_x() + bar.get_width()/2., height + max(values)*0.01,
    #                 f'{value:,}', ha='center', va='bottom')
    
    plt.tight_layout()
    plt.show()



# Your existing functions (create_households, solve_households) should be available
# Along with solve_system.solve_household





def main():
    # Test parameters - replace with your actual parameters
    test_parameters = [12, -.2, -.2, 0.2, 0.4, 0.3]  # Example values

    # Run speed test
    results = simple_household_speed_test(
        parameters=test_parameters,
        n_test_households=500,  # Start small for testing
    )
    # Plot results
    plot_simple_results(results)

    # Print summary
    if 'feasibility' in results:
        feas = results['feasibility']
        print(f"\nSUMMARY:")
        print(f"Best setup: {feas['best_cores']} cores")
        print(f"Feasible MH iterations per week: {feas['feasible_iterations_per_week']:,}")

if __name__ == "__main__":
    main()