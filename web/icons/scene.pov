#version 3.7;
#include "rad_def.inc"

global_settings {
  charset utf8
  assumed_gamma 1.0
  radiosity {
    Rad_Settings(Radiosity_Final, off, off)
  }
}

#ifndef (shadow_pass)
  #declare shadow_pass = -1;
#end

camera {
  orthographic
  location  <-1, -6, 4>
  up z
  sky z
  look_at   <0, 0, 1>
  right x*(image_width/image_height)
  angle 30
}

light_source {
  <20, -30, 100>
  color rgb 1
  area_light 100*x 100*y  6, 6 jitter orient circular adaptive 1
}

plane {
  z, 0.0
  texture {
    pigment { color rgb 1 }
    finish { diffuse 1 }
  }
  #if (shadow_pass = 0)
    no_image
  #end
}

#include "shapes.inc"

#macro Statue(Font, Caption)
Center_Object(
  intersection {
    text {
      ttf Font Caption 0.2, 0
      rotate <90, 0, 0>
      scale 2
      #if (shadow_pass = 1)
        no_image
      #end
    }
    box { <-10, -10, 0>, <10, 10, 10> }
    texture {
      pigment { color rgb 0.8 }
      finish {
        diffuse 1
      }
    }
  }, x + y)
#end
