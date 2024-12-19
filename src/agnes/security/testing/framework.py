from typing import Dict, Any, List, Optional
import aiohttp
import asyncio
import ssl
import jwt
from datetime import datetime
import socket
import subprocess
from dataclasses import dataclass

@dataclass
class SecurityTestResult:
    test_name: str
    status: str
    description: str
    severity: str
    recommendations: List[str]

class SecurityTestFramework:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.results: List[SecurityTestResult] = []
    
    async def run_all_tests(self) -> List[SecurityTestResult]:
        """Run all security tests"""
        tests = [
            self.test_ssl_configuration(),
            self.test_jwt_security(),
            self.test_api_endpoints(),
            self.test_network_security(),
            self.test_dependency_vulnerabilities()
        ]
        
        results = await asyncio.gather(*tests)
        self.results.extend([r for test_results in results 
                           for r in test_results])
        return self.results
    
    async def test_ssl_configuration(self) -> List[SecurityTestResult]:
        """Test SSL/TLS configuration"""
        results = []
        
        # Test SSL version
        ctx = ssl.create_default_context()
        try:
            conn = ctx.wrap_socket(
                socket.socket(),
                server_hostname=self.config["host"]
            )
            conn.connect((self.config["host"], 443))
            ssl_version = conn.version()
            
            results.append(SecurityTestResult(
                test_name="SSL Version Check",
                status="Pass" if ssl_version in ["TLSv1.2", "TLSv1.3"] 
                else "Fail",
                description=f"SSL Version: {ssl_version}",
                severity="High",
                recommendations=[
                    "Use TLS 1.2 or higher",
                    "Disable older SSL/TLS versions"
                ]
            ))
            
        except Exception as e:
            results.append(SecurityTestResult(
                test_name="SSL Configuration",
                status="Error",
                description=str(e),
                severity="High",
                recommendations=["Verify SSL configuration"]
            ))
        
        return results
    
    async def test_jwt_security(self) -> List[SecurityTestResult]:
        """Test JWT security configuration"""
        results = []
        
        # Test token generation and validation
        try:
            # Generate test token
            test_token = jwt.encode(
                {"test": "data", "exp": datetime.utcnow()},
                self.config["jwt_secret"]
            )
            
            # Test with invalid token
            async with aiohttp.ClientSession() as session:
                resp = await session.get(
                    f"{self.config['api_url']}/test",
                    headers={"Authorization": f"Bearer invalid_token"}
                )
                
                results.append(SecurityTestResult(
                    test_name="JWT Invalid Token",
                    status="Pass" if resp.status == 401 else "Fail",
                    description="Testing invalid JWT token handling",
                    severity="High",
                    recommendations=[
                        "Ensure proper JWT validation",
                        "Implement token expiration"
                    ]
                ))
        
        except Exception as e:
            results.append(SecurityTestResult(
                test_name="JWT Security",
                status="Error",
                description=str(e),
                severity="High",
                recommendations=["Verify JWT configuration"]
            ))
        
        return results
    
    async def test_api_endpoints(self) -> List[SecurityTestResult]:
        """Test API endpoint security"""
        results = []
        
        # Test endpoints for common vulnerabilities
        endpoints = self.config.get("test_endpoints", ["/api/test"])
        
        async with aiohttp.ClientSession() as session:
            for endpoint in endpoints:
                # Test SQL injection
                try:
                    resp = await session.get(
                        f"{self.config['api_url']}{endpoint}",
                        params={"id": "1' OR '1'='1"}
                    )
                    results.append(SecurityTestResult(
                        test_name=f"SQL Injection - {endpoint}",
                        status="Pass" if resp.status != 200 else "Fail",
                        description="Testing SQL injection vulnerability",
                        severity="Critical",
                        recommendations=[
                            "Use parameterized queries",
                            "Implement input validation"
                        ]
                    ))
                except Exception as e:
                    results.append(SecurityTestResult(
                        test_name=f"API Security - {endpoint}",
                        status="Error",
                        description=str(e),
                        severity="High",
                        recommendations=["Verify API security"]
                    ))
        
        return results
    
    async def test_network_security(self) -> List[SecurityTestResult]:
        """Test network security configuration"""
        results = []
        
        # Run network security checks
        try:
            # Test open ports
            process = subprocess.Popen(
                ["nmap", "-p-", self.config["host"]],
                stdout=subprocess.PIPE
            )
            output, _ = process.communicate()
            
            results.append(SecurityTestResult(
                test_name="Open Ports Scan",
                status="Info",
                description=output.decode(),
                severity="Medium",
                recommendations=[
                    "Close unnecessary ports",
                    "Implement firewall rules"
                ]
            ))
            
        except Exception as e:
            results.append(SecurityTestResult(
                test_name="Network Security",
                status="Error",
                description=str(e),
                severity="High",
                recommendations=["Verify network security"]
            ))
        
        return results
    
    async def test_dependency_vulnerabilities(self) -> List[SecurityTestResult]:
        """Test dependencies for known vulnerabilities"""
        results = []
        
        try:
            # Run safety check on Python dependencies
            process = subprocess.Popen(
                ["safety", "check"],
                stdout=subprocess.PIPE
            )
            output, _ = process.communicate()
            
            results.append(SecurityTestResult(
                test_name="Dependency Security",
                status="Info",
                description=output.decode(),
                severity="High",
                recommendations=[
                    "Update vulnerable dependencies",
                    "Implement dependency scanning in CI/CD"
                ]
            ))
            
        except Exception as e:
            results.append(SecurityTestResult(
                test_name="Dependency Check",
                status="Error",
                description=str(e),
                severity="High",
                recommendations=["Verify dependency security"]
            ))
        
        return results
