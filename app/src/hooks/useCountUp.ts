import { useState, useEffect, useRef } from 'react';

interface UseCountUpOptions {
  duration?: number;
  decimals?: number;
  enabled?: boolean;
}

export function useCountUp(
  targetValue: number | null,
  options: UseCountUpOptions = {}
): number {
  const { duration = 1000, decimals = 0, enabled = true } = options;
  const [count, setCount] = useState(0);
  const animationFrameRef = useRef<number>();
  const startTimeRef = useRef<number>();
  const startValueRef = useRef(0);
  const targetValueRef = useRef<number | null>(null);
  const countRef = useRef(0);

  useEffect(() => {
    countRef.current = count;
  }, [count]);

  useEffect(() => {
    if (!enabled || targetValue === null) {
      setCount(0);
      return;
    }

    if (targetValueRef.current !== targetValue) {
      startValueRef.current = countRef.current;
      targetValueRef.current = targetValue;
      startTimeRef.current = undefined;
    }

    const animate = (currentTime: number) => {
      if (!startTimeRef.current) {
        startTimeRef.current = currentTime;
      }

      const elapsed = currentTime - startTimeRef.current;
      const progress = Math.min(elapsed / duration, 1);

      const easeOut = 1 - Math.pow(1 - progress, 3);

      const currentValue = startValueRef.current + (targetValue - startValueRef.current) * easeOut;
      
      if (decimals > 0) {
        setCount(Number(currentValue.toFixed(decimals)));
      } else {
        setCount(Math.round(currentValue));
      }

      if (progress < 1) {
        animationFrameRef.current = requestAnimationFrame(animate);
      } else {
        if (decimals > 0) {
          setCount(Number(targetValue.toFixed(decimals)));
        } else {
          setCount(Math.round(targetValue));
        }
      }
    };

    animationFrameRef.current = requestAnimationFrame(animate);

    return () => {
      if (animationFrameRef.current) {
        cancelAnimationFrame(animationFrameRef.current);
      }
    };
  }, [targetValue, duration, decimals, enabled]);

  return count;
}

