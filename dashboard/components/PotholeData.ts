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
      lat: 10.7769,
      lng: 106.7009,
      address: "Nguyễn Huệ, District 1, Ho Chi Minh City"
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
      lat: 10.7829,
      lng: 106.6660,
      address: "Võ Văn Tần, District 3, Ho Chi Minh City"
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
      lat: 10.8014,
      lng: 106.6509,
      address: "Cách Mạng Tháng Tám, District 10, Ho Chi Minh City"
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
      lat: 10.7623,
      lng: 106.6822,
      address: "Lê Lai, District 1, Ho Chi Minh City"
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
      lat: 10.8543,
      lng: 106.6291,
      address: "Phạm Văn Đồng, Bình Thạnh District, Ho Chi Minh City"
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
      lat: 10.7717,
      lng: 106.6980,
      address: "Nguyễn Thị Minh Khai, District 1, Ho Chi Minh City"
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
      lat: 10.7328,
      lng: 106.7233,
      address: "Điện Biên Phủ, Bình Thạnh District, Ho Chi Minh City"
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
      lat: 10.7906,
      lng: 106.7145,
      address: "Đinh Tiên Hoàng, Bình Thạnh District, Ho Chi Minh City"
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