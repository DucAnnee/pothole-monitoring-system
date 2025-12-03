export interface Pothole {
  id: number;
  position: {
    lat: number;
    lng: number;
    address: string;
  };
  severity: number; // 1-10 scale
  measurements: {
    depth: number; // in cm
    width: number; // in cm
    length: number; // in cm
  };
  images: {
    normal: string;
    birdEye: string;
  };
  dateReported: string;
  status: 'reported' | 'in-progress' | 'fixed';
}

export const potholes: Pothole[] = [
  {
    id: 1,
    position: {
      lat: 40.7489,
      lng: -73.9680,
      address: "5th Ave & E 45th St, New York, NY"
    },
    severity: 9,
    measurements: {
      depth: 15,
      width: 45,
      length: 60
    },
    images: {
      normal: "https://images.unsplash.com/photo-1709934730506-fba12664d4e4?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxkYW1hZ2VkJTIwcm9hZCUyMHBvdGhvbGV8ZW58MXx8fHwxNzYyMTUxMzYxfDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-10-28",
    status: 'reported'
  },
  {
    id: 2,
    position: {
      lat: 40.7580,
      lng: -73.9855,
      address: "Broadway & W 47th St, New York, NY"
    },
    severity: 6,
    measurements: {
      depth: 8,
      width: 30,
      length: 40
    },
    images: {
      normal: "https://images.unsplash.com/photo-1636367167117-1c584316f6eb?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzdHJlZXQlMjBwb3Rob2xlJTIwdmlld3xlbnwxfHx8fDE3NjIxNTEzNjJ8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-10-30",
    status: 'in-progress'
  },
  {
    id: 3,
    position: {
      lat: 40.7614,
      lng: -73.9776,
      address: "Park Ave & E 51st St, New York, NY"
    },
    severity: 4,
    measurements: {
      depth: 5,
      width: 20,
      length: 25
    },
    images: {
      normal: "https://images.unsplash.com/photo-1741996950842-c3a280a438a4?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxhc3BoYWx0JTIwY3JhY2slMjBkYW1hZ2V8ZW58MXx8fHwxNzYyMTUxMzYyfDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-11-01",
    status: 'reported'
  },
  {
    id: 4,
    position: {
      lat: 40.7527,
      lng: -73.9772,
      address: "Madison Ave & E 44th St, New York, NY"
    },
    severity: 7,
    measurements: {
      depth: 12,
      width: 35,
      length: 50
    },
    images: {
      normal: "https://images.unsplash.com/photo-1709934730506-fba12664d4e4?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxkYW1hZ2VkJTIwcm9hZCUyMHBvdGhvbGV8ZW58MXx8fHwxNzYyMTUxMzYxfDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-10-25",
    status: 'reported'
  },
  {
    id: 5,
    position: {
      lat: 40.7545,
      lng: -73.9917,
      address: "7th Ave & W 48th St, New York, NY"
    },
    severity: 3,
    measurements: {
      depth: 4,
      width: 18,
      length: 22
    },
    images: {
      normal: "https://images.unsplash.com/photo-1636367167117-1c584316f6eb?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzdHJlZXQlMjBwb3Rob2xlJTIwdmlld3xlbnwxfHx8fDE3NjIxNTEzNjJ8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-11-02",
    status: 'fixed'
  },
  {
    id: 6,
    position: {
      lat: 40.7498,
      lng: -73.9765,
      address: "Lexington Ave & E 43rd St, New York, NY"
    },
    severity: 8,
    measurements: {
      depth: 14,
      width: 42,
      length: 55
    },
    images: {
      normal: "https://images.unsplash.com/photo-1741996950842-c3a280a438a4?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxhc3BoYWx0JTIwY3JhY2slMjBkYW1hZ2V8ZW58MXx8fHwxNzYyMTUxMzYyfDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-10-26",
    status: 'in-progress'
  },
  {
    id: 7,
    position: {
      lat: 40.7596,
      lng: -73.9700,
      address: "3rd Ave & E 50th St, New York, NY"
    },
    severity: 5,
    measurements: {
      depth: 7,
      width: 28,
      length: 35
    },
    images: {
      normal: "https://images.unsplash.com/photo-1709934730506-fba12664d4e4?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxkYW1hZ2VkJTIwcm9hZCUyMHBvdGhvbGV8ZW58MXx8fHwxNzYyMTUxMzYxfDA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-10-29",
    status: 'reported'
  },
  {
    id: 8,
    position: {
      lat: 40.7561,
      lng: -73.9860,
      address: "8th Ave & W 49th St, New York, NY"
    },
    severity: 10,
    measurements: {
      depth: 18,
      width: 50,
      length: 70
    },
    images: {
      normal: "https://images.unsplash.com/photo-1636367167117-1c584316f6eb?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxzdHJlZXQlMjBwb3Rob2xlJTIwdmlld3xlbnwxfHx8fDE3NjIxNTEzNjJ8MA&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral",
      birdEye: "https://images.unsplash.com/photo-1678125511633-ea2098515bab?crop=entropy&cs=tinysrgb&fit=max&fm=jpg&ixid=M3w3Nzg4Nzd8MHwxfHNlYXJjaHwxfHxyb2FkJTIwZGFtYWdlJTIwYWVyaWFsfGVufDF8fHx8MTc2MjE1MTM2MXww&ixlib=rb-4.1.0&q=80&w=1080&utm_source=figma&utm_medium=referral"
    },
    dateReported: "2025-10-24",
    status: 'reported'
  }
];
