import { X, MapPin, AlertCircle, Calendar, Activity } from 'lucide-react';
import { Card } from './ui/card';
import { Badge } from './ui/badge';
import { Button } from './ui/button';
import { Separator } from './ui/separator';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import type { Pothole } from './PotholeData';
import { ImageWithFallback } from './figma/ImageWithFallback';

interface PotholeDetailsProps {
  pothole: Pothole;
  onClose: () => void;
}

export function PotholeDetails({ pothole, onClose }: PotholeDetailsProps) {
  const getSeverityColor = (severity: number) => {
    if (severity >= 8) return 'bg-red-500';
    if (severity >= 5) return 'bg-orange-500';
    return 'bg-yellow-500';
  };

  const getSeverityLabel = (severity: number) => {
    if (severity >= 8) return 'Critical';
    if (severity >= 5) return 'Moderate';
    return 'Minor';
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'reported': return 'bg-red-100 text-red-800 border-red-200';
      case 'in-progress': return 'bg-blue-100 text-blue-800 border-blue-200';
      case 'fixed': return 'bg-green-100 text-green-800 border-green-200';
      default: return 'bg-gray-100 text-gray-800 border-gray-200';
    }
  };

  return (
    <div className="w-full h-full overflow-auto p-6 bg-white">
      <div className="flex items-start justify-between mb-6">
        <div>
          <h2 className="mb-2">Pothole Details</h2>
          <p className="text-gray-500">ID: #{pothole.id}</p>
        </div>
        <Button variant="ghost" size="icon" onClick={onClose}>
          <X className="w-5 h-5" />
        </Button>
      </div>

      <Card className="p-4 mb-6">
        <div className="flex items-center justify-between mb-4">
          <div className="flex items-center gap-2">
            <AlertCircle className="w-5 h-5 text-red-500" />
            <span>Severity Level</span>
          </div>
          <div className="flex items-center gap-2">
            <div className={`w-12 h-12 rounded-full flex items-center justify-center ${getSeverityColor(pothole.severity)}`}>
              <span className="text-white">{pothole.severity}</span>
            </div>
            <Badge variant="outline">{getSeverityLabel(pothole.severity)}</Badge>
          </div>
        </div>
        <Separator className="my-4" />
        <div className="space-y-3">
          <div className="flex items-center gap-2">
            <Activity className="w-5 h-5 text-gray-500" />
            <span className="text-gray-500">Status:</span>
            <Badge variant="outline" className={getStatusColor(pothole.status)}>
              {pothole.status.replace('-', ' ').toUpperCase()}
            </Badge>
          </div>
          <div className="flex items-center gap-2">
            <Calendar className="w-5 h-5 text-gray-500" />
            <span className="text-gray-500">Reported:</span>
            <span>{new Date(pothole.dateReported).toLocaleDateString()}</span>
          </div>
        </div>
      </Card>

      <Card className="p-4 mb-6">
        <div className="flex items-start gap-2 mb-3">
          <MapPin className="w-5 h-5 text-gray-500 mt-1" />
          <div>
            <h3 className="mb-1">Location</h3>
            <p className="text-gray-600">{pothole.position.address}</p>
            <p className="text-gray-400 mt-1">
              {pothole.position.lat.toFixed(6)}, {pothole.position.lng.toFixed(6)}
            </p>
          </div>
        </div>
      </Card>

      <Card className="p-4 mb-6">
        <h3 className="mb-4">Measurements</h3>
        <div className="grid grid-cols-3 gap-4">
          <div className="text-center p-3 bg-gray-50 rounded-lg">
            <div className="text-gray-500 mb-1">Depth</div>
            <div className="text-red-500">{pothole.measurements.depth} cm</div>
          </div>
          <div className="text-center p-3 bg-gray-50 rounded-lg">
            <div className="text-gray-500 mb-1">Width</div>
            <div className="text-red-500">{pothole.measurements.width} cm</div>
          </div>
          <div className="text-center p-3 bg-gray-50 rounded-lg">
            <div className="text-gray-500 mb-1">Length</div>
            <div className="text-red-500">{pothole.measurements.length} cm</div>
          </div>
        </div>
      </Card>

      <Card className="p-4">
        <h3 className="mb-4">Images</h3>
        <Tabs defaultValue="normal" className="w-full">
          <TabsList className="w-full grid grid-cols-2">
            <TabsTrigger value="normal">Normal View</TabsTrigger>
            <TabsTrigger value="bird">Bird's Eye View</TabsTrigger>
          </TabsList>
          <TabsContent value="normal" className="mt-4">
            <div className="relative rounded-lg overflow-hidden bg-gray-100">
              <ImageWithFallback
                src={pothole.images.normal}
                alt="Normal view of pothole"
                className="w-full h-64 object-cover"
              />
            </div>
          </TabsContent>
          <TabsContent value="bird" className="mt-4">
            <div className="relative rounded-lg overflow-hidden bg-gray-100">
              <ImageWithFallback
                src={pothole.images.birdEye}
                alt="Bird's eye view of pothole"
                className="w-full h-64 object-cover"
              />
            </div>
          </TabsContent>
        </Tabs>
      </Card>
    </div>
  );
}
