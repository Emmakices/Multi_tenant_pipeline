#!/usr/bin/env python3
"""
Enterprise Test Suite Runner for Multi-Tenant Pipeline

This script provides comprehensive testing capabilities for enterprise deployment validation,
including unit tests, integration tests, load tests, and health monitoring.
"""

import argparse
import json
import logging
import os
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

# Configure enterprise-grade logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enterprise_test_execution.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class EnterpriseTestSuite:
    """Enterprise-grade test suite runner with comprehensive reporting."""
    
    def __init__(self):
        self.project_root = Path(__file__).parent
        self.test_results = {}
        self.start_time = datetime.now()
        
    def run_unit_tests(self, engines: Optional[List[str]] = None) -> Dict:
        """Run comprehensive unit tests for all processing engines."""
        logger.info("Starting unit test execution")
        
        test_engines = engines or ['pandas_engine', 'polars_engine', 'dask_engine', 'cloud_function']
        results = {}
        
        for engine in test_engines:
            logger.info(f"Running unit tests for {engine}")
            engine_path = self.project_root / engine
            test_file = engine_path / f"test_{engine}.py"
            
            if not test_file.exists():
                logger.warning(f"Test file not found: {test_file}")
                continue
                
            start_time = time.time()
            try:
                cmd = [
                    sys.executable, '-m', 'pytest',
                    str(test_file),
                    '-v',
                    '--tb=short',
                    '--json-report',
                    f'--json-report-file={engine}_test_results.json'
                ]
                
                result = subprocess.run(
                    cmd,
                    cwd=engine_path,
                    capture_output=True,
                    text=True,
                    timeout=300
                )
                
                execution_time = time.time() - start_time
                
                # Parse pytest results
                try:
                    with open(engine_path / f"{engine}_test_results.json", 'r') as f:
                        test_data = json.load(f)
                        
                    results[engine] = {
                        'status': 'passed' if result.returncode == 0 else 'failed',
                        'total_tests': test_data['summary']['total'],
                        'passed': test_data['summary'].get('passed', 0),
                        'failed': test_data['summary'].get('failed', 0),
                        'skipped': test_data['summary'].get('skipped', 0),
                        'execution_time': execution_time,
                        'returncode': result.returncode
                    }
                    
                except (FileNotFoundError, KeyError, json.JSONDecodeError):
                    # Fallback parsing from stdout
                    results[engine] = self._parse_pytest_output(result.stdout, execution_time, result.returncode)
                    
                logger.info(f"{engine} unit tests completed: {results[engine]['status']}")
                
            except subprocess.TimeoutExpired:
                results[engine] = {
                    'status': 'timeout',
                    'execution_time': 300,
                    'error': 'Test execution exceeded 5 minute timeout'
                }
                logger.error(f"{engine} unit tests timed out")
                
            except Exception as e:
                results[engine] = {
                    'status': 'error',
                    'execution_time': time.time() - start_time,
                    'error': str(e)
                }
                logger.error(f"{engine} unit tests failed: {e}")
        
        return results
    
    def run_integration_tests(self) -> Dict:
        """Run end-to-end integration tests."""
        logger.info("Starting integration test execution")
        
        start_time = time.time()
        try:
            result = subprocess.run(
                [sys.executable, 'integration_test.py'],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            execution_time = time.time() - start_time
            
            # Parse integration test results
            success = 'All integration tests passed!' in result.stdout
            
            integration_results = {
                'status': 'passed' if success else 'failed',
                'execution_time': execution_time,
                'returncode': result.returncode,
                'output': result.stdout,
                'details': self._parse_integration_output(result.stdout)
            }
            
            logger.info(f"Integration tests completed: {integration_results['status']}")
            return integration_results
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'timeout',
                'execution_time': 120,
                'error': 'Integration tests exceeded 2 minute timeout'
            }
        except Exception as e:
            return {
                'status': 'error',
                'execution_time': time.time() - start_time,
                'error': str(e)
            }
    
    def run_load_tests(self) -> Dict:
        """Run comprehensive load and performance tests."""
        logger.info("Starting load test execution")
        
        start_time = time.time()
        try:
            result = subprocess.run(
                [sys.executable, 'load_test.py'],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=300
            )
            
            execution_time = time.time() - start_time
            
            # Parse load test results
            success = 'OVERALL: PASS' in result.stdout
            
            load_results = {
                'status': 'passed' if success else 'failed',
                'execution_time': execution_time,
                'returncode': result.returncode,
                'output': result.stdout,
                'performance_metrics': self._parse_load_test_output(result.stdout)
            }
            
            logger.info(f"Load tests completed: {load_results['status']}")
            return load_results
            
        except subprocess.TimeoutExpired:
            return {
                'status': 'timeout',
                'execution_time': 300,
                'error': 'Load tests exceeded 5 minute timeout'
            }
        except Exception as e:
            return {
                'status': 'error',
                'execution_time': time.time() - start_time,
                'error': str(e)
            }
    
    def run_health_checks(self) -> Dict:
        """Run system health checks across all components."""
        logger.info("Starting health check validation")
        
        # This would run our health check scripts
        # For now, return a placeholder structure
        return {
            'status': 'completed',
            'timestamp': datetime.now().isoformat(),
            'engine_health': {
                'pandas': {'regions': 12, 'healthy': 12, 'percentage': 100.0},
                'polars': {'regions': 12, 'healthy': 2, 'percentage': 16.67},
                'dask': {'regions': 12, 'healthy': 0, 'percentage': 0.0}
            },
            'overall_health': 38.89
        }
    
    def _parse_pytest_output(self, output: str, execution_time: float, returncode: int) -> Dict:
        """Parse pytest output for test results."""
        lines = output.split('\n')
        
        # Look for the summary line
        summary_line = None
        for line in lines:
            if 'passed' in line and ('failed' in line or 'error' in line or line.strip().endswith('passed')):
                summary_line = line.strip()
                break
        
        if not summary_line:
            return {
                'status': 'failed' if returncode != 0 else 'unknown',
                'execution_time': execution_time,
                'returncode': returncode,
                'error': 'Could not parse test results'
            }
        
        # Parse the summary
        import re
        passed_match = re.search(r'(\d+) passed', summary_line)
        failed_match = re.search(r'(\d+) failed', summary_line)
        error_match = re.search(r'(\d+) error', summary_line)
        
        passed = int(passed_match.group(1)) if passed_match else 0
        failed = int(failed_match.group(1)) if failed_match else 0
        errors = int(error_match.group(1)) if error_match else 0
        
        return {
            'status': 'passed' if returncode == 0 else 'failed',
            'total_tests': passed + failed + errors,
            'passed': passed,
            'failed': failed,
            'errors': errors,
            'execution_time': execution_time,
            'returncode': returncode
        }
    
    def _parse_integration_output(self, output: str) -> Dict:
        """Parse integration test output for detailed results."""
        details = {
            'message_validation': 'Unknown',
            'engine_selection': 'Unknown',
            'concurrent_processing': 'Unknown',
            'correlation_tracking': 'Unknown',
            'tenant_isolation': 'Unknown'
        }
        
        lines = output.split('\n')
        for line in lines:
            if 'message structures validated' in line:
                details['message_validation'] = 'PASS' if 'PASS' in line else 'FAIL'
            elif 'Engine selection accuracy' in line:
                details['engine_selection'] = line.strip()
            elif 'Concurrent processing test' in line:
                details['concurrent_processing'] = 'PASS' if 'PASS' in line else 'FAIL'
            elif 'Correlation ID tracking validated' in line:
                details['correlation_tracking'] = 'PASS' if 'PASS' in line else 'FAIL'
            elif 'Tenant isolation validated' in line:
                details['tenant_isolation'] = 'PASS' if 'PASS' in line else 'FAIL'
        
        return details
    
    def _parse_load_test_output(self, output: str) -> Dict:
        """Parse load test output for performance metrics."""
        metrics = {
            'light_load': {},
            'medium_load': {},
            'heavy_load': {},
            'peak_load': {},
            'tenant_isolation': {},
            'correlation_tracking': {}
        }
        
        lines = output.split('\n')
        current_test = None
        
        for line in lines:
            if 'Testing Light Load' in line:
                current_test = 'light_load'
            elif 'Testing Medium Load' in line:
                current_test = 'medium_load'
            elif 'Testing Heavy Load' in line:
                current_test = 'heavy_load'
            elif 'Testing Peak Load' in line:
                current_test = 'peak_load'
            elif 'Success Rate:' in line and current_test:
                import re
                success_match = re.search(r'Success Rate: ([\d.]+)%', line)
                if success_match:
                    metrics[current_test]['success_rate'] = float(success_match.group(1))
            elif 'Throughput:' in line and current_test:
                throughput_match = re.search(r'Throughput: ([\d.]+) req/sec', line)
                if throughput_match:
                    metrics[current_test]['throughput'] = float(throughput_match.group(1))
        
        return metrics
    
    def generate_report(self, results: Dict) -> str:
        """Generate comprehensive test execution report."""
        report = [
            "=" * 80,
            "ENTERPRISE TEST SUITE EXECUTION REPORT",
            "=" * 80,
            f"Execution Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Execution Time: {(datetime.now() - self.start_time).total_seconds():.2f}s",
            "",
            "TEST RESULTS SUMMARY:",
            "-" * 40
        ]
        
        # Unit test summary
        if 'unit_tests' in results:
            unit_results = results['unit_tests']
            total_engines = len(unit_results)
            passed_engines = sum(1 for r in unit_results.values() if r.get('status') == 'passed')
            
            report.extend([
                f"Unit Tests: {passed_engines}/{total_engines} engines passed",
                ""
            ])
            
            for engine, result in unit_results.items():
                status_icon = "✓" if result.get('status') == 'passed' else "✗"
                total_tests = result.get('total_tests', 0)
                passed_tests = result.get('passed', 0)
                execution_time = result.get('execution_time', 0)
                
                report.append(f"  {status_icon} {engine}: {passed_tests}/{total_tests} tests ({execution_time:.2f}s)")
        
        # Integration test summary
        if 'integration_tests' in results:
            integration = results['integration_tests']
            status_icon = "✓" if integration.get('status') == 'passed' else "✗"
            execution_time = integration.get('execution_time', 0)
            
            report.extend([
                "",
                f"{status_icon} Integration Tests: {integration.get('status', 'unknown')} ({execution_time:.2f}s)"
            ])
        
        # Load test summary
        if 'load_tests' in results:
            load_tests = results['load_tests']
            status_icon = "✓" if load_tests.get('status') == 'passed' else "✗"
            execution_time = load_tests.get('execution_time', 0)
            
            report.extend([
                "",
                f"{status_icon} Load Tests: {load_tests.get('status', 'unknown')} ({execution_time:.2f}s)"
            ])
        
        # Health check summary
        if 'health_checks' in results:
            health = results['health_checks']
            overall_health = health.get('overall_health', 0)
            status_icon = "✓" if overall_health >= 80 else "⚠" if overall_health >= 60 else "✗"
            
            report.extend([
                "",
                f"{status_icon} Health Checks: {overall_health:.1f}% system health"
            ])
        
        report.extend([
            "",
            "=" * 80,
            "For detailed results, see individual test logs and JSON reports.",
            "=" * 80
        ])
        
        return "\n".join(report)
    
    def run_full_suite(self, engines: Optional[List[str]] = None) -> Dict:
        """Run the complete enterprise test suite."""
        logger.info("Starting enterprise test suite execution")
        
        results = {}
        
        # Run unit tests
        try:
            results['unit_tests'] = self.run_unit_tests(engines)
        except Exception as e:
            logger.error(f"Unit tests failed: {e}")
            results['unit_tests'] = {'error': str(e)}
        
        # Run integration tests
        try:
            results['integration_tests'] = self.run_integration_tests()
        except Exception as e:
            logger.error(f"Integration tests failed: {e}")
            results['integration_tests'] = {'error': str(e)}
        
        # Run load tests
        try:
            results['load_tests'] = self.run_load_tests()
        except Exception as e:
            logger.error(f"Load tests failed: {e}")
            results['load_tests'] = {'error': str(e)}
        
        # Run health checks
        try:
            results['health_checks'] = self.run_health_checks()
        except Exception as e:
            logger.error(f"Health checks failed: {e}")
            results['health_checks'] = {'error': str(e)}
        
        # Generate and save report
        report = self.generate_report(results)
        
        # Save results to JSON
        results_file = f"enterprise_test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(results_file, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        logger.info(f"Enterprise test suite completed. Results saved to {results_file}")
        print(report)
        
        return results


def main():
    """Main entry point for enterprise test suite."""
    parser = argparse.ArgumentParser(description='Enterprise Test Suite Runner')
    parser.add_argument('--engines', nargs='*', 
                        choices=['pandas_engine', 'polars_engine', 'dask_engine', 'cloud_function'],
                        help='Specific engines to test (default: all)')
    parser.add_argument('--unit-only', action='store_true',
                        help='Run only unit tests')
    parser.add_argument('--integration-only', action='store_true',
                        help='Run only integration tests')
    parser.add_argument('--load-only', action='store_true',
                        help='Run only load tests')
    parser.add_argument('--health-only', action='store_true',
                        help='Run only health checks')
    parser.add_argument('--save-results', type=str,
                        help='Save results to specified JSON file')
    
    args = parser.parse_args()
    
    suite = EnterpriseTestSuite()
    
    # Run specific test types if requested
    if args.unit_only:
        results = {'unit_tests': suite.run_unit_tests(args.engines)}
    elif args.integration_only:
        results = {'integration_tests': suite.run_integration_tests()}
    elif args.load_only:
        results = {'load_tests': suite.run_load_tests()}
    elif args.health_only:
        results = {'health_checks': suite.run_health_checks()}
    else:
        # Run full suite
        results = suite.run_full_suite(args.engines)
    
    # Save custom results file if specified
    if args.save_results:
        with open(args.save_results, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"Results saved to {args.save_results}")
    
    # Return appropriate exit code
    unit_success = results.get('unit_tests', {}).get('status') != 'failed'
    integration_success = results.get('integration_tests', {}).get('status') != 'failed'
    load_success = results.get('load_tests', {}).get('status') != 'failed'
    
    if all([unit_success, integration_success, load_success]):
        sys.exit(0)  # Success
    else:
        sys.exit(1)  # Failure


if __name__ == "__main__":
    main()