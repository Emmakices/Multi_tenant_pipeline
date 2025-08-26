#!/usr/bin/env python3
"""
Parallel Test Execution Script for Multi-Tenant Pipeline

This script provides optimized test execution with the following features:
- Parallel test execution across multiple engines
- Selective test running (by engine, category, or tag)
- Performance monitoring and reporting
- Coverage aggregation
- Failure analysis and reporting
"""

import sys
import os
import subprocess
import multiprocessing
import argparse
import time
import json
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('test_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class TestRunner:
    """Optimized test runner for multi-tenant pipeline."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.engines = ['pandas_engine', 'polars_engine', 'dask_engine', 'router_function']
        self.results = {}
        
    def run_engine_tests(self, engine: str, test_args: List[str] = None) -> Dict:
        """Run tests for a specific engine."""
        engine_path = self.project_root / engine
        if not engine_path.exists():
            return {
                'engine': engine,
                'status': 'skipped',
                'reason': f'Engine directory {engine} not found',
                'duration': 0,
                'test_count': 0,
                'failures': 0
            }
        
        # Default test arguments
        default_args = [
            '-v',
            '--tb=short',
            '--durations=10',
            f'--cov={engine_path.name}/main',
            '--cov-report=term-missing',
            f'--cov-report=html:{engine_path}/htmlcov',
            f'--cov-report=xml:{engine_path}/coverage.xml',
        ]
        
        # Add parallel execution if available
        try:
            import pytest_xdist
            default_args.extend(['-n', 'auto'])
        except ImportError:
            logger.warning(f"pytest-xdist not available for {engine}, running sequentially")
        
        if test_args:
            default_args.extend(test_args)
        
        # Find test file
        test_file = engine_path / f'test_{engine}.py'
        if not test_file.exists():
            return {
                'engine': engine,
                'status': 'skipped',
                'reason': f'Test file test_{engine}.py not found',
                'duration': 0,
                'test_count': 0,
                'failures': 0
            }
        
        logger.info(f"🚀 Starting tests for {engine}")
        start_time = time.time()
        
        try:
            # Run pytest
            cmd = [sys.executable, '-m', 'pytest', str(test_file)] + default_args
            result = subprocess.run(
                cmd,
                cwd=engine_path,
                capture_output=True,
                text=True,
                timeout=600  # 10 minute timeout
            )
            
            duration = time.time() - start_time
            
            # Parse results
            output_lines = result.stdout.split('\n')
            test_summary_line = [line for line in output_lines if 'passed' in line and ('failed' in line or 'error' in line or 'warnings' in line)]
            
            test_count = 0
            failures = 0
            warnings = 0
            
            if test_summary_line:
                summary = test_summary_line[-1]
                if 'passed' in summary:
                    # Extract numbers from summary line
                    import re
                    numbers = re.findall(r'(\d+)\s+(\w+)', summary)
                    for count, status in numbers:
                        if status in ['passed', 'failed', 'error', 'skipped']:
                            test_count += int(count)
                        if status in ['failed', 'error']:
                            failures += int(count)
                        if 'warning' in status:
                            warnings += int(count)
            
            status = 'passed' if result.returncode == 0 else 'failed'
            
            return {
                'engine': engine,
                'status': status,
                'duration': duration,
                'test_count': test_count,
                'failures': failures,
                'warnings': warnings,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode
            }
            
        except subprocess.TimeoutExpired:
            duration = time.time() - start_time
            return {
                'engine': engine,
                'status': 'timeout',
                'duration': duration,
                'test_count': 0,
                'failures': 1,
                'warnings': 0,
                'stdout': '',
                'stderr': f'Tests timed out after {duration:.1f} seconds'
            }
        
        except Exception as e:
            duration = time.time() - start_time
            return {
                'engine': engine,
                'status': 'error',
                'duration': duration,
                'test_count': 0,
                'failures': 1,
                'warnings': 0,
                'stdout': '',
                'stderr': str(e)
            }
    
    def run_parallel_tests(self, engines: List[str] = None, test_args: List[str] = None, max_workers: int = None) -> Dict:
        """Run tests in parallel across multiple engines."""
        if engines is None:
            engines = self.engines
        
        if max_workers is None:
            max_workers = min(len(engines), multiprocessing.cpu_count())
        
        logger.info(f"🔥 Running tests in parallel across {len(engines)} engines with {max_workers} workers")
        start_time = time.time()
        
        results = {}
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all test jobs
            future_to_engine = {
                executor.submit(self.run_engine_tests, engine, test_args): engine 
                for engine in engines
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_engine):
                engine = future_to_engine[future]
                try:
                    result = future.result()
                    results[engine] = result
                    
                    # Log immediate result
                    status_emoji = "✅" if result['status'] == 'passed' else "❌"
                    logger.info(f"{status_emoji} {engine}: {result['status']} in {result['duration']:.1f}s "
                              f"({result['test_count']} tests, {result['failures']} failures)")
                    
                except Exception as e:
                    logger.error(f"❌ {engine}: Exception occurred: {e}")
                    results[engine] = {
                        'engine': engine,
                        'status': 'error',
                        'duration': 0,
                        'test_count': 0,
                        'failures': 1,
                        'warnings': 0,
                        'stderr': str(e)
                    }
        
        total_duration = time.time() - start_time
        
        # Aggregate results
        summary = {
            'total_duration': total_duration,
            'engines': results,
            'summary': {
                'total_tests': sum(r.get('test_count', 0) for r in results.values()),
                'total_failures': sum(r.get('failures', 0) for r in results.values()),
                'total_warnings': sum(r.get('warnings', 0) for r in results.values()),
                'engines_passed': sum(1 for r in results.values() if r.get('status') == 'passed'),
                'engines_failed': sum(1 for r in results.values() if r.get('status') in ['failed', 'error', 'timeout']),
                'engines_skipped': sum(1 for r in results.values() if r.get('status') == 'skipped')
            }
        }
        
        return summary
    
    def run_sequential_tests(self, engines: List[str] = None, test_args: List[str] = None) -> Dict:
        """Run tests sequentially for debugging or resource-constrained environments."""
        if engines is None:
            engines = self.engines
        
        logger.info(f"🐌 Running tests sequentially across {len(engines)} engines")
        start_time = time.time()
        
        results = {}
        
        for engine in engines:
            logger.info(f"Running tests for {engine}...")
            result = self.run_engine_tests(engine, test_args)
            results[engine] = result
            
            status_emoji = "✅" if result['status'] == 'passed' else "❌"
            logger.info(f"{status_emoji} {engine}: {result['status']} in {result['duration']:.1f}s "
                      f"({result['test_count']} tests, {result['failures']} failures)")
        
        total_duration = time.time() - start_time
        
        # Aggregate results (same structure as parallel)
        summary = {
            'total_duration': total_duration,
            'engines': results,
            'summary': {
                'total_tests': sum(r.get('test_count', 0) for r in results.values()),
                'total_failures': sum(r.get('failures', 0) for r in results.values()),
                'total_warnings': sum(r.get('warnings', 0) for r in results.values()),
                'engines_passed': sum(1 for r in results.values() if r.get('status') == 'passed'),
                'engines_failed': sum(1 for r in results.values() if r.get('status') in ['failed', 'error', 'timeout']),
                'engines_skipped': sum(1 for r in results.values() if r.get('status') == 'skipped')
            }
        }
        
        return summary
    
    def print_summary(self, results: Dict):
        """Print a comprehensive test summary."""
        summary = results['summary']
        
        print("\n" + "="*80)
        print("🧪 TEST EXECUTION SUMMARY")
        print("="*80)
        
        print(f"⏱️  Total Duration: {results['total_duration']:.1f}s")
        print(f"🎯 Total Tests: {summary['total_tests']}")
        print(f"✅ Engines Passed: {summary['engines_passed']}")
        print(f"❌ Engines Failed: {summary['engines_failed']}")
        print(f"⏭️  Engines Skipped: {summary['engines_skipped']}")
        
        if summary['total_failures'] > 0:
            print(f"💥 Total Failures: {summary['total_failures']}")
        
        if summary['total_warnings'] > 0:
            print(f"⚠️  Total Warnings: {summary['total_warnings']}")
        
        print("\n📊 ENGINE BREAKDOWN:")
        print("-" * 80)
        
        for engine, result in results['engines'].items():
            status = result['status']
            duration = result.get('duration', 0)
            test_count = result.get('test_count', 0)
            failures = result.get('failures', 0)
            warnings = result.get('warnings', 0)
            
            status_emoji = {
                'passed': '✅',
                'failed': '❌',
                'error': '💥',
                'timeout': '⏰',
                'skipped': '⏭️'
            }.get(status, '❓')
            
            print(f"{status_emoji} {engine:15} | {status:8} | {duration:6.1f}s | "
                  f"{test_count:3d} tests | {failures:2d} failures | {warnings:2d} warnings")
        
        print("-" * 80)
        
        # Overall status
        if summary['engines_failed'] == 0:
            print("🎉 ALL TESTS PASSED!")
        else:
            print(f"💔 {summary['engines_failed']} ENGINE(S) FAILED")
            print("\n❌ FAILED ENGINES:")
            for engine, result in results['engines'].items():
                if result['status'] in ['failed', 'error', 'timeout']:
                    print(f"   • {engine}: {result.get('stderr', 'Unknown error')}")
        
        print("="*80)
    
    def save_results(self, results: Dict, filename: str = 'test_results.json'):
        """Save test results to JSON file."""
        output_file = self.project_root / filename
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"📁 Test results saved to {output_file}")


def main():
    parser = argparse.ArgumentParser(description='Multi-Tenant Pipeline Test Runner')
    parser.add_argument('--engines', nargs='*', default=None,
                       help='Engines to test (pandas_engine, polars_engine, dask_engine, router_function)')
    parser.add_argument('--parallel', action='store_true', default=True,
                       help='Run tests in parallel (default: True)')
    parser.add_argument('--sequential', action='store_true',
                       help='Run tests sequentially (overrides --parallel)')
    parser.add_argument('--max-workers', type=int, default=None,
                       help='Maximum number of parallel workers')
    parser.add_argument('--markers', help='Pytest markers to filter tests (e.g., "unit", "integration")')
    parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    parser.add_argument('--save-results', default='test_results.json',
                       help='Save results to JSON file')
    parser.add_argument('--fast', action='store_true', help='Run only fast tests (< 5 seconds)')
    parser.add_argument('--coverage', action='store_true', default=True, help='Generate coverage reports')
    
    args = parser.parse_args()
    
    # Setup test arguments
    test_args = []
    
    if args.markers:
        test_args.extend(['-m', args.markers])
    
    if args.fast:
        test_args.extend(['-m', 'fast'])
    
    if args.verbose:
        test_args.append('-vv')
    
    if not args.coverage:
        # Remove coverage arguments - we'd need to modify the runner for this
        pass
    
    # Initialize runner
    runner = TestRunner()
    
    # Determine which engines to test
    engines_to_test = args.engines if args.engines else runner.engines
    
    # Validate engines
    valid_engines = [e for e in engines_to_test if (runner.project_root / e).exists()]
    if len(valid_engines) != len(engines_to_test):
        invalid = set(engines_to_test) - set(valid_engines)
        logger.warning(f"⚠️  Invalid engines skipped: {invalid}")
    
    if not valid_engines:
        logger.error("❌ No valid engines found to test")
        return 1
    
    # Run tests
    if args.sequential:
        results = runner.run_sequential_tests(valid_engines, test_args)
    else:
        results = runner.run_parallel_tests(valid_engines, test_args, args.max_workers)
    
    # Print summary
    runner.print_summary(results)
    
    # Save results
    if args.save_results:
        runner.save_results(results, args.save_results)
    
    # Exit with appropriate code
    return 0 if results['summary']['engines_failed'] == 0 else 1


if __name__ == '__main__':
    sys.exit(main())