from typing import Dict, Any, List, Optional, Union, BinaryIO
import asyncio
import pandas as pd
import json
import csv
import yaml
from datetime import datetime
import aiohttp
import aioboto3
from dataclasses import dataclass
import logging
from enum import Enum
import jinja2
import pdfkit
import xlsxwriter
import io

class ExportFormat(Enum):
    JSON = "json"
    CSV = "csv"
    EXCEL = "excel"
    PDF = "pdf"
    HTML = "html"

@dataclass
class ExportTask:
    id: str
    format: ExportFormat
    query: Dict[str, Any]
    filters: Optional[Dict[str, Any]] = None
    template: Optional[str] = None
    callback_url: Optional[str] = None
    created_at: datetime = None
    completed_at: Optional[datetime] = None
    status: str = "pending"
    error: Optional[str] = None

class DataExporter:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.storage = ExportStorage(config['storage'])
        self.logger = logging.getLogger(__name__)
        self.template_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('templates/exports')
        )
    
    async def export_data(self, task: ExportTask) -> str:
        """Export data according to task"""
        try:
            # Fetch data
            data = await self._fetch_data(task.query, task.filters)
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Apply template if specified
            if task.template:
                template = self.template_env.get_template(task.template)
                context = {'data': data}
                content = template.render(context)
            else:
                content = None
            
            # Export in specified format
            if task.format == ExportFormat.JSON:
                file_path = await self._export_json(task.id, df)
            elif task.format == ExportFormat.CSV:
                file_path = await self._export_csv(task.id, df)
            elif task.format == ExportFormat.EXCEL:
                file_path = await self._export_excel(task.id, df)
            elif task.format == ExportFormat.PDF:
                file_path = await self._export_pdf(task.id, content or df)
            elif task.format == ExportFormat.HTML:
                file_path = await self._export_html(task.id, content or df)
            
            # Store export file
            url = await self.storage.store_file(file_path, task.format)
            
            # Send callback if specified
            if task.callback_url:
                await self._send_callback(task, url)
            
            return url
        
        except Exception as e:
            self.logger.error(f"Export failed: {e}")
            raise
    
    async def _fetch_data(self,
                         query: Dict[str, Any],
                         filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Fetch data from source"""
        # Implementation depends on data source
        # This is a placeholder
        return []
    
    async def _export_json(self, task_id: str, df: pd.DataFrame) -> str:
        """Export data as JSON"""
        file_path = f"/tmp/export_{task_id}.json"
        df.to_json(file_path, orient='records', date_format='iso')
        return file_path
    
    async def _export_csv(self, task_id: str, df: pd.DataFrame) -> str:
        """Export data as CSV"""
        file_path = f"/tmp/export_{task_id}.csv"
        df.to_csv(file_path, index=False)
        return file_path
    
    async def _export_excel(self, task_id: str, df: pd.DataFrame) -> str:
        """Export data as Excel"""
        file_path = f"/tmp/export_{task_id}.xlsx"
        
        with pd.ExcelWriter(
            file_path,
            engine='xlsxwriter'
        ) as writer:
            df.to_excel(writer, sheet_name='Data', index=False)
            
            # Get workbook and worksheet
            workbook = writer.book
            worksheet = writer.sheets['Data']
            
            # Add formats
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#D3D3D3',
                'border': 1
            })
            
            # Format header row
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Auto-adjust columns
            for column in df:
                column_width = max(
                    df[column].astype(str).map(len).max(),
                    len(column)
                )
                col_idx = df.columns.get_loc(column)
                worksheet.set_column(col_idx, col_idx, column_width)
        
        return file_path
    
    async def _export_pdf(self,
                         task_id: str,
                         content: Union[str, pd.DataFrame]) -> str:
        """Export data as PDF"""
        file_path = f"/tmp/export_{task_id}.pdf"
        
        if isinstance(content, pd.DataFrame):
            html = content.to_html(
                index=False,
                classes='table table-striped'
            )
        else:
            html = content
        
        options = {
            'page-size': 'A4',
            'margin-top': '0.75in',
            'margin-right': '0.75in',
            'margin-bottom': '0.75in',
            'margin-left': '0.75in'
        }
        
        pdfkit.from_string(html, file_path, options=options)
        return file_path
    
    async def _export_html(self,
                          task_id: str,
                          content: Union[str, pd.DataFrame]) -> str:
        """Export data as HTML"""
        file_path = f"/tmp/export_{task_id}.html"
        
        if isinstance(content, pd.DataFrame):
            html = content.to_html(
                index=False,
                classes='table table-striped'
            )
        else:
            html = content
        
        with open(file_path, 'w') as f:
            f.write(html)
        
        return file_path
    
    async def _send_callback(self, task: ExportTask, url: str):
        """Send callback notification"""
        payload = {
            'task_id': task.id,
            'status': 'completed',
            'url': url,
            'format': task.format.value,
            'completed_at': datetime.utcnow().isoformat()
        }
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    task.callback_url,
                    json=payload
                ) as response:
                    if response.status not in (200, 201):
                        self.logger.error(
                            f"Callback failed: {await response.text()}"
                        )
            except Exception as e:
                self.logger.error(f"Callback failed: {e}")

class ExportStorage:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.type = config['type']
        self.session = aioboto3.Session()
    
    async def store_file(self,
                        file_path: str,
                        format: ExportFormat) -> str:
        """Store export file"""
        if self.type == 's3':
            return await self._store_s3(file_path, format)
        else:
            return file_path
    
    async def _store_s3(self, file_path: str, format: ExportFormat) -> str:
        """Store file in S3"""
        bucket = self.config['bucket']
        key = f"exports/{datetime.utcnow().strftime('%Y/%m/%d')}/{file_path.split('/')[-1]}"
        
        async with self.session.client('s3') as s3:
            with open(file_path, 'rb') as f:
                await s3.upload_fileobj(
                    f,
                    bucket,
                    key,
                    ExtraArgs={
                        'ContentType': self._get_content_type(format)
                    }
                )
        
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    
    def _get_content_type(self, format: ExportFormat) -> str:
        """Get content type for format"""
        content_types = {
            ExportFormat.JSON: 'application/json',
            ExportFormat.CSV: 'text/csv',
            ExportFormat.EXCEL: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            ExportFormat.PDF: 'application/pdf',
            ExportFormat.HTML: 'text/html'
        }
        return content_types.get(format, 'application/octet-stream')

class ExportManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.exporter = DataExporter(config)
        self.tasks: Dict[str, ExportTask] = {}
        self.logger = logging.getLogger(__name__)
    
    async def create_export(self,
                          format: ExportFormat,
                          query: Dict[str, Any],
                          filters: Optional[Dict[str, Any]] = None,
                          template: Optional[str] = None,
                          callback_url: Optional[str] = None) -> ExportTask:
        """Create new export task"""
        task = ExportTask(
            id=str(uuid.uuid4()),
            format=format,
            query=query,
            filters=filters,
            template=template,
            callback_url=callback_url,
            created_at=datetime.utcnow()
        )
        
        self.tasks[task.id] = task
        asyncio.create_task(self._process_task(task))
        
        return task
    
    async def _process_task(self, task: ExportTask):
        """Process export task"""
        try:
            task.status = "processing"
            url = await self.exporter.export_data(task)
            
            task.status = "completed"
            task.completed_at = datetime.utcnow()
            
            self.logger.info(
                f"Export task {task.id} completed: {url}"
            )
        
        except Exception as e:
            task.status = "failed"
            task.error = str(e)
            self.logger.error(f"Export task {task.id} failed: {e}")
    
    def get_task_status(self, task_id: str) -> Optional[ExportTask]:
        """Get export task status"""
        return self.tasks.get(task_id)
