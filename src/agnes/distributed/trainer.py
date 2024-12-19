from typing import Dict, Any, List, Optional
import torch
import torch.distributed as dist
from torch.nn.parallel import DistributedDataParallel
from dataclasses import dataclass

@dataclass
class TrainingConfig:
    batch_size: int
    learning_rate: float
    num_epochs: int
    num_gpus: int
    distributed: bool
    checkpoint_dir: str
    gradient_accumulation_steps: int

class DistributedTrainer:
    def __init__(self, config: TrainingConfig):
        self.config = config
        self.model = None
        self.optimizer = None
        self._setup_distributed()
    
    def _setup_distributed(self):
        """Initialize distributed training environment"""
        if self.config.distributed:
            dist.init_process_group(backend='nccl')
            self.device = torch.device(f'cuda:{dist.get_rank()}')
            torch.cuda.set_device(self.device)
    
    async def setup_model(self, model: torch.nn.Module):
        """Setup model for distributed training"""
        self.model = model.to(self.device)
        if self.config.distributed:
            self.model = DistributedDataParallel(
                self.model,
                device_ids=[self.device]
            )
        
        self.optimizer = torch.optim.Adam(
            self.model.parameters(),
            lr=self.config.learning_rate
        )
    
    async def train_epoch(self, 
                         dataloader: torch.utils.data.DataLoader) -> Dict[str, float]:
        """Train for one epoch"""
        self.model.train()
        total_loss = 0.0
        steps = 0
        
        for batch_idx, batch in enumerate(dataloader):
            # Move batch to device
            batch = {k: v.to(self.device) for k, v in batch.items()}
            
            # Forward pass
            outputs = self.model(**batch)
            loss = outputs.loss / self.config.gradient_accumulation_steps
            
            # Backward pass
            loss.backward()
            
            # Gradient accumulation
            if (batch_idx + 1) % self.config.gradient_accumulation_steps == 0:
                self.optimizer.step()
                self.optimizer.zero_grad()
            
            total_loss += loss.item()
            steps += 1
            
        return {
            "average_loss": total_loss / steps,
            "steps": steps
        }
    
    async def save_checkpoint(self, epoch: int, loss: float):
        """Save training checkpoint"""
        if dist.get_rank() == 0:  # Only save on master process
            checkpoint = {
                'epoch': epoch,
                'model_state_dict': self.model.state_dict(),
                'optimizer_state_dict': self.optimizer.state_dict(),
                'loss': loss,
            }
            torch.save(
                checkpoint,
                f"{self.config.checkpoint_dir}/checkpoint_epoch_{epoch}.pt"
            )
    
    async def load_checkpoint(self, checkpoint_path: str):
        """Load training checkpoint"""
        checkpoint = torch.load(checkpoint_path, map_location=self.device)
        self.model.load_state_dict(checkpoint['model_state_dict'])
        self.optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
        return checkpoint['epoch'], checkpoint['loss']
